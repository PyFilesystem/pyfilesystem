"""
fs.s3fs
=======

**Currently only avaiable on Python2 due to boto not being available for Python3**

FS subclass accessing files in Amazon S3

This module provides the class 'S3FS', which implements the FS filesystem
interface for objects stored in Amazon Simple Storage Service (S3).

"""

import os
import datetime
import tempfile
from fnmatch import fnmatch
import stat as statinfo

import boto.s3.connection
from boto.s3.prefix import Prefix
from boto.exception import S3ResponseError

from fs.base import *
from fs.path import *
from fs.errors import *
from fs.remote import *
from fs.filelike import LimitBytesFile
from fs import iotools

import six

# Boto is not thread-safe, so we need to use a per-thread S3 connection.
if hasattr(threading,"local"):
    thread_local = threading.local
else:
    class thread_local(object):
        def __init__(self):
            self._map = {}
        def __getattr__(self,attr):
            try:
                return self._map[(threading.currentThread(),attr)]
            except KeyError:
                raise AttributeError, attr
        def __setattr__(self,attr,value):
            self._map[(threading.currentThread(),attr)] = value


class S3FS(FS):
    """A filesystem stored in Amazon S3.

    This class provides the FS interface for files stored in Amazon's Simple
    Storage Service (S3).  It should be instantiated with the name of the
    S3 bucket to use, and optionally a prefix under which the files should
    be stored.

    Local temporary files are used when opening files from this filesystem,
    and any changes are only pushed back into S3 when the files are closed
    or flushed.
    """

    _meta = {'thread_safe': True,
             'virtual': False,
             'read_only': False,
             'unicode_paths': True,
             'case_insensitive_paths': False,
             'network': True,
             'atomic.move': True,
             'atomic.copy': True,
             'atomic.makedir': True,
             'atomic.rename': False,
             'atomic.setcontents': True
             }

    class meta:
        PATH_MAX = None
        NAME_MAX = None

    def __init__(self, bucket, prefix="", aws_access_key=None, aws_secret_key=None, separator="/", thread_synchronize=True, key_sync_timeout=1):
        """Constructor for S3FS objects.

        S3FS objects require the name of the S3 bucket in which to store
        files, and can optionally be given a prefix under which the files
        should be stored.  The AWS public and private keys may be specified
        as additional arguments; if they are not specified they will be
        read from the two environment variables AWS_ACCESS_KEY_ID and
        AWS_SECRET_ACCESS_KEY.

        The keyword argument 'key_sync_timeout' specifies the maximum
        time in seconds that the filesystem will spend trying to confirm
        that a newly-uploaded S3 key is available for reading.  For no
        timeout set it to zero.  To disable these checks entirely (and
        thus reduce the filesystem's consistency guarantees to those of
        S3's "eventual consistency" model) set it to None.

        By default the path separator is "/", but this can be overridden
        by specifying the keyword 'separator' in the constructor.
        """
        self._bucket_name = bucket
        self._access_keys = (aws_access_key,aws_secret_key)
        self._separator = separator
        self._key_sync_timeout = key_sync_timeout
        # Normalise prefix to this form: path/to/files/
        prefix = normpath(prefix)
        while prefix.startswith(separator):
            prefix = prefix[1:]
        if not prefix.endswith(separator) and prefix != "":
            prefix = prefix + separator
        if isinstance(prefix,unicode):
            prefix = prefix.encode("utf8")
        if aws_access_key is None:
            if "AWS_ACCESS_KEY_ID" not in os.environ:
                raise CreateFailedError("AWS_ACCESS_KEY_ID not set")
        if aws_secret_key is None:
            if "AWS_SECRET_ACCESS_KEY" not in os.environ:
                raise CreateFailedError("AWS_SECRET_ACCESS_KEY not set")
        self._prefix = prefix
        self._tlocal = thread_local()
        super(S3FS, self).__init__(thread_synchronize=thread_synchronize)

    #  Make _s3conn and _s3bukt properties that are created on demand,
    #  since they cannot be stored during pickling.

    def _s3conn(self):
        try:
            (c,ctime) = self._tlocal.s3conn
            if time.time() - ctime > 60:
                raise AttributeError
            return c
        except AttributeError:
            c = boto.s3.connection.S3Connection(*self._access_keys)
            self._tlocal.s3conn = (c,time.time())
            return c
    _s3conn = property(_s3conn)

    def _s3bukt(self):
        try:
            (b,ctime) = self._tlocal.s3bukt
            if time.time() - ctime > 60:
                raise AttributeError
            return b
        except AttributeError:
            try:
                # Validate by listing the bucket if there is no prefix.
                # If there is a prefix, validate by listing only the prefix
                # itself, to avoid errors when an IAM policy has been applied.
                if self._prefix:
                    b = self._s3conn.get_bucket(self._bucket_name, validate=0)
                    b.get_key(self._prefix)
                else:
                    b = self._s3conn.get_bucket(self._bucket_name, validate=1)
            except S3ResponseError, e:
                if "404 Not Found" not in str(e):
                    raise
                b = self._s3conn.create_bucket(self._bucket_name)
            self._tlocal.s3bukt = (b,time.time())
            return b
    _s3bukt = property(_s3bukt)

    def __getstate__(self):
        state = super(S3FS,self).__getstate__()
        del state['_tlocal']
        return state

    def __setstate__(self,state):
        super(S3FS,self).__setstate__(state)
        self._tlocal = thread_local()

    def __repr__(self):
        args = (self.__class__.__name__,self._bucket_name,self._prefix)
        return '<%s: %s:%s>' % args

    __str__ = __repr__

    def _s3path(self,path):
        """Get the absolute path to a file stored in S3."""
        path = relpath(normpath(path))
        path = self._separator.join(iteratepath(path))
        s3path = self._prefix + path
        if s3path and s3path[-1] == self._separator:
            s3path = s3path[:-1]
        if isinstance(s3path,unicode):
            s3path = s3path.encode("utf8")
        return s3path

    def _uns3path(self,s3path,roots3path=None):
        """Get the local path for a file stored in S3.

        This is essentially the opposite of self._s3path().
        """
        if roots3path is None:
            roots3path = self._s3path("")
        i = len(roots3path)
        return s3path[i:]

    def _sync_key(self,k):
        """Synchronise on contents of the given key.

        Since S3 only offers "eventual consistency" of data, it is possible
        to create a key but be unable to read it back straight away.  This
        method works around that limitation by polling the key until it reads
        back the value expected by the given key.

        Note that this could easily fail if the key is modified by another
        program, meaning the content will never be as specified in the given
        key.  This is the reason for the timeout argument to the construtcor.
        """
        timeout = self._key_sync_timeout
        if timeout is None:
            return k
        k2 = self._s3bukt.get_key(k.name)
        t = time.time()
        while k2 is None or k2.etag != k.etag:
            if timeout > 0:
                if t + timeout < time.time():
                    break
            time.sleep(0.1)
            k2 = self._s3bukt.get_key(k.name)
        return k2

    def _sync_set_contents(self,key,contents):
        """Synchronously set the contents of a key."""
        if isinstance(key,basestring):
            key = self._s3bukt.new_key(key)
        if isinstance(contents,basestring):
            key.set_contents_from_string(contents)
        elif hasattr(contents,"md5"):
            hexmd5 = contents.md5
            b64md5 = hexmd5.decode("hex").encode("base64").strip()
            key.set_contents_from_file(contents,md5=(hexmd5,b64md5))
        else:
            try:
                contents.seek(0)
            except (AttributeError,EnvironmentError):
                tf = tempfile.TemporaryFile()
                data = contents.read(524288)
                while data:
                    tf.write(data)
                    data = contents.read(524288)
                tf.seek(0)
                key.set_contents_from_file(tf)
            else:
                key.set_contents_from_file(contents)
        return self._sync_key(key)

    def makepublic(self, path):
        """Mark given path as publicly accessible using HTTP(S)"""
        s3path = self._s3path(path)
        k = self._s3bukt.get_key(s3path)
        k.make_public()

    def getpathurl(self, path, allow_none=False, expires=3600):
        """Returns a url that corresponds to the given path."""
        s3path = self._s3path(path)
        k = self._s3bukt.get_key(s3path)

        # Is there AllUsers group with READ permissions?
        is_public = True in [grant.permission == 'READ' and
                grant.uri == 'http://acs.amazonaws.com/groups/global/AllUsers'
                for grant in k.get_acl().acl.grants]

        url = k.generate_url(expires, force_http=is_public)

        if url == None:
            if not allow_none:
                raise NoPathURLError(path=path)
            return None

        if is_public:
            # Strip time token; it has no sense for public resource
            url = url.split('?')[0]

        return url

    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=64*1024):
        s3path = self._s3path(path)
        if isinstance(data, six.text_type):
            data = data.encode(encoding=encoding, errors=errors)
        self._sync_set_contents(s3path, data)

    @iotools.filelike_to_stream
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        """Open the named file in the given mode.

        This method downloads the file contents into a local temporary file
        so that it can be worked on efficiently.  Any changes made to the
        file are only sent back to S3 when the file is flushed or closed.
        """
        if self.isdir(path):
            raise ResourceInvalidError(path)
        s3path = self._s3path(path)
        # Truncate the file if requested
        if "w" in mode:
            k = self._sync_set_contents(s3path,"")
        else:
            k = self._s3bukt.get_key(s3path)
        if k is None:
            # Create the file if it's missing
            if "w" not in mode and "a" not in mode:
                raise ResourceNotFoundError(path)
            if not self.isdir(dirname(path)):
                raise ParentDirectoryMissingError(path)
            k = self._sync_set_contents(s3path,"")
        #  Make sure nothing tries to read past end of socket data
        f = LimitBytesFile(k.size,k,"r")
        #  For streaming reads, return the key object directly
        if mode == "r-":
            return f
        #  For everything else, use a RemoteFileBuffer.
        #  This will take care of closing the socket when it's done.
        return RemoteFileBuffer(self,path,mode,f)

    def exists(self,path):
        """Check whether a path exists."""
        s3path = self._s3path(path)
        s3pathD = s3path + self._separator
        # The root directory always exists
        if self._prefix.startswith(s3path):
            return True
        ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        for k in ks:
            # A regular file
            if _eq_utf8(k.name,s3path):
                return True
            # A directory
            if _eq_utf8(k.name,s3pathD):
                return True
        return False

    def isdir(self,path):
        """Check whether a path exists and is a directory."""
        s3path = self._s3path(path) + self._separator
        # Root is always a directory
        if s3path == "/" or s3path == self._prefix:
            return True
        # Use a list request so that we return true if there are any files
        # in that directory.  This avoids requiring a special file for the
        # the directory itself, which other tools may not create.
        ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        try:
            iter(ks).next()
        except StopIteration:
            return False
        else:
            return True

    def isfile(self,path):
        """Check whether a path exists and is a regular file."""
        s3path = self._s3path(path)
        # Root is never a file
        if self._prefix.startswith(s3path):
            return False
        k = self._s3bukt.get_key(s3path)
        if k is not None:
            return True
        return False

    def listdir(self,path="./",wildcard=None,full=False,absolute=False,
                               dirs_only=False,files_only=False):
        """List contents of a directory."""
        return list(self.ilistdir(path,wildcard,full,absolute,
                                       dirs_only,files_only))

    def listdirinfo(self,path="./",wildcard=None,full=False,absolute=False,
                                   dirs_only=False,files_only=False):
        return list(self.ilistdirinfo(path,wildcard,full,absolute,
                                           dirs_only,files_only))

    def ilistdir(self,path="./",wildcard=None,full=False,absolute=False,
                                dirs_only=False,files_only=False):
        """List contents of a directory."""
        keys = self._iter_keys(path)
        entries = self._filter_keys(path,keys,wildcard,full,absolute,
                                         dirs_only,files_only)
        return (nm for (nm,k) in entries)

    def ilistdirinfo(self,path="./",wildcard=None,full=False,absolute=False,
                                    dirs_only=False,files_only=False):
        keys = self._iter_keys(path)
        entries = self._filter_keys(path,keys,wildcard,full,absolute,
                                         dirs_only,files_only)
        return ((nm,self._get_key_info(k,nm)) for (nm,k) in entries)

    def _iter_keys(self,path):
        """Iterator over keys contained in the given directory.

        This generator yields (name,key) pairs for each entry in the given
        directory.  If the path is not a directory, it raises the approprate
        error.
        """
        s3path = self._s3path(path) + self._separator
        if s3path == "/":
            s3path = ""
        isDir = False
        for k in self._s3bukt.list(prefix=s3path,delimiter=self._separator):
            if not isDir:
                isDir = True
            # Skip over the entry for the directory itself, if it exists
            name = self._uns3path(k.name,s3path)
            if name != "":
                if not isinstance(name,unicode):
                    name = name.decode("utf8")
                if name.endswith(self._separator):
                    name = name[:-1]
                yield (name,k)
        if not isDir:
            if s3path != self._prefix:
                if self.isfile(path):
                    msg = "that's not a directory: %(path)s"
                    raise ResourceInvalidError(path,msg=msg)
                raise ResourceNotFoundError(path)

    def _key_is_dir(self, k):
        if isinstance(k,Prefix):
            return True
        if k.name.endswith(self._separator):
            return True
        return False

    def _filter_keys(self,path,keys,wildcard,full,absolute,
                               dirs_only,files_only):
        """Filter out keys not matching the given criteria.

        Given a (name,key) iterator as returned by _iter_keys, this method
        applies the given filtering criteria and returns a filtered iterator.
        """
        sep = self._separator
        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only can not both be True")
        if dirs_only:
            keys = ((nm,k) for (nm,k) in keys if self._key_is_dir(k))
        elif files_only:
            keys = ((nm,k) for (nm,k) in keys if not self._key_is_dir(k))
        if wildcard is not None:
            if callable(wildcard):
                keys = ((nm,k) for (nm,k) in keys if wildcard(nm))
            else:
                keys = ((nm,k) for (nm,k) in keys if fnmatch(nm,wildcard))
        if full:
            return ((relpath(pathjoin(path, nm)),k) for (nm,k) in keys)
        elif absolute:
            return ((abspath(pathjoin(path, nm)),k) for (nm,k) in keys)
        return keys

    def makedir(self,path,recursive=False,allow_recreate=False):
        """Create a directory at the given path.

        The 'mode' argument is accepted for compatibility with the standard
        FS interface, but is currently ignored.
        """
        s3path = self._s3path(path)
        s3pathD = s3path + self._separator
        if s3pathD == self._prefix:
            if allow_recreate:
                return
            msg = "Can not create a directory that already exists"\
                  " (try allow_recreate=True): %(path)s"
            raise DestinationExistsError(path, msg=msg)
        s3pathP = self._s3path(dirname(path))
        if s3pathP:
            s3pathP = s3pathP + self._separator
        # Check various preconditions using list of parent dir
        ks = self._s3bukt.list(prefix=s3pathP,delimiter=self._separator)
        if s3pathP == self._prefix:
            parentExists = True
        else:
            parentExists = False
        for k in ks:
            if not parentExists:
                parentExists = True
            if _eq_utf8(k.name,s3path):
                # It's already a file
                msg = "Destination exists as a regular file: %(path)s"
                raise ResourceInvalidError(path, msg=msg)
            if _eq_utf8(k.name,s3pathD):
                # It's already a directory
                if allow_recreate:
                    return
                msg = "Can not create a directory that already exists"\
                      " (try allow_recreate=True): %(path)s"
                raise DestinationExistsError(path, msg=msg)
        # Create parent if required
        if not parentExists:
            if recursive:
                self.makedir(dirname(path),recursive,allow_recreate)
            else:
                msg = "Parent directory does not exist: %(path)s"
                raise ParentDirectoryMissingError(path, msg=msg)
        # Create an empty file representing the directory
        if s3pathD not in ('/', ''):
            self._sync_set_contents(s3pathD,"")

    def remove(self,path):
        """Remove the file at the given path."""
        s3path = self._s3path(path)
        ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        for k in ks:
            if _eq_utf8(k.name,s3path):
                break
            if _startswith_utf8(k.name,s3path + "/"):
                msg = "that's not a file: %(path)s"
                raise ResourceInvalidError(path,msg=msg)
        else:
            raise ResourceNotFoundError(path)
        self._s3bukt.delete_key(s3path)
        k = self._s3bukt.get_key(s3path)
        while k:
            k = self._s3bukt.get_key(s3path)

    def removedir(self,path,recursive=False,force=False):
        """Remove the directory at the given path."""
        if normpath(path) in ('', '/'):
            raise RemoveRootError(path)
        s3path = self._s3path(path)
        if s3path != self._prefix:
            s3path = s3path + self._separator
        if force:
            #  If we will be forcibly removing any directory contents, we
            #  might as well get the un-delimited list straight away.
            ks = self._s3bukt.list(prefix=s3path)
        else:
            ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        # Fail if the directory is not empty, or remove them if forced
        found = False
        for k in ks:
            found = True
            if not _eq_utf8(k.name,s3path):
                if not force:
                    raise DirectoryNotEmptyError(path)
                self._s3bukt.delete_key(k.name)
        if not found:
            if self.isfile(path):
                msg = "removedir() called on a regular file: %(path)s"
                raise ResourceInvalidError(path,msg=msg)
            if path not in ("","/"):
                raise ResourceNotFoundError(path)
        self._s3bukt.delete_key(s3path)
        if recursive and path not in ("","/"):
            pdir = dirname(path)
            try:
                self.removedir(pdir,recursive=True,force=False)
            except DirectoryNotEmptyError:
                pass

    def rename(self,src,dst):
        """Rename the file at 'src' to 'dst'."""
        # Actually, in S3 'rename' is exactly the same as 'move'
        if self.isfile(src):
            self.move(src,dst)
        else:
            self.movedir(src,dst)

    def getinfo(self,path):
        s3path = self._s3path(path)
        if path in ("","/"):
            k = Prefix(bucket=self._s3bukt,name="/")
        else:
            k = self._s3bukt.get_key(s3path)
            if k is None:
                ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
                for k in ks:
                    if isinstance(k,Prefix):
                        break
                else:
                    raise ResourceNotFoundError(path)
        return self._get_key_info(k,path)

    def _get_key_info(self,key,name=None):
        info = {}
        if name is not None:
            info["name"] = basename(name)
        else:
            info["name"] = basename(self._uns3key(k.name))
        if self._key_is_dir(key):
            info["st_mode"] = 0700 | statinfo.S_IFDIR
        else:
            info["st_mode"] =  0700 | statinfo.S_IFREG
        if hasattr(key,"size"):
            info['size'] = int(key.size)
        etag = getattr(key,"etag",None)
        if etag is not None:
            if isinstance(etag,unicode):
               etag = etag.encode("utf8")
            info['etag'] = etag.strip('"').strip("'")
        if hasattr(key,"last_modified"):
            # TODO: does S3 use any other formats?
            fmt = "%a, %d %b %Y %H:%M:%S %Z"
            try:
                mtime = datetime.datetime.strptime(key.last_modified,fmt)
                info['modified_time'] = mtime
            except ValueError:
                pass
        return info

    def desc(self,path):
        return "No description available"

    def copy(self,src,dst,overwrite=False,chunk_size=16384):
        """Copy a file from 'src' to 'dst'.

        src -- The source path
        dst -- The destination path
        overwrite -- If True, then the destination may be overwritten
        (if a file exists at that location). If False then an exception will be
        thrown if the destination exists
        chunk_size -- Size of chunks to use in copy (ignored by S3)
        """
        s3path_dst = self._s3path(dst)
        s3path_dstD = s3path_dst + self._separator
        #  Check for various preconditions.
        ks = self._s3bukt.list(prefix=s3path_dst,delimiter=self._separator)
        dstOK = False
        for k in ks:
            # It exists as a regular file
            if _eq_utf8(k.name,s3path_dst):
                if not overwrite:
                    raise DestinationExistsError(dst)
                dstOK = True
                break
            # Check if it refers to a directory.  If so, we copy *into* it.
            # Since S3 lists in lexicographic order, subsequent iterations
            # of the loop will check for the existence of the new filename.
            if _eq_utf8(k.name,s3path_dstD):
                nm = basename(src)
                dst = pathjoin(dirname(dst),nm)
                s3path_dst = s3path_dstD + nm
                dstOK = True
        if not dstOK and not self.isdir(dirname(dst)):
            msg = "Destination directory does not exist: %(path)s"
            raise ParentDirectoryMissingError(dst,msg=msg)
        # OK, now we can copy the file.
        s3path_src = self._s3path(src)
        try:
            self._s3bukt.copy_key(s3path_dst,self._bucket_name,s3path_src)
        except S3ResponseError, e:
            if "404 Not Found" in str(e):
                msg = "Source is not a file: %(path)s"
                raise ResourceInvalidError(src, msg=msg)
            raise e
        else:
            k = self._s3bukt.get_key(s3path_dst)
            while k is None:
                k = self._s3bukt.get_key(s3path_dst)
            self._sync_key(k)

    def move(self,src,dst,overwrite=False,chunk_size=16384):
        """Move a file from one location to another."""
        self.copy(src,dst,overwrite=overwrite)
        self._s3bukt.delete_key(self._s3path(src))

    def walkfiles(self,
              path="/",
              wildcard=None,
              dir_wildcard=None,
              search="breadth",
              ignore_errors=False ):
        if search != "breadth" or dir_wildcard is not None:
            args = (wildcard,dir_wildcard,search,ignore_errors)
            for item in super(S3FS,self).walkfiles(path,*args):
                yield item
        else:
            prefix = self._s3path(path)
            for k in self._s3bukt.list(prefix=prefix):
                name = relpath(self._uns3path(k.name,prefix))
                if name != "":
                    if not isinstance(name,unicode):
                        name = name.decode("utf8")
                    if not k.name.endswith(self._separator):
                        if wildcard is not None:
                            if callable(wildcard):
                                if not wildcard(basename(name)):
                                    continue
                            else:
                                if not fnmatch(basename(name),wildcard):
                                    continue
                        yield pathjoin(path,name)


    def walkinfo(self,
              path="/",
              wildcard=None,
              dir_wildcard=None,
              search="breadth",
              ignore_errors=False ):
        if search != "breadth" or dir_wildcard is not None:
            args = (wildcard,dir_wildcard,search,ignore_errors)
            for item in super(S3FS,self).walkfiles(path,*args):
                yield (item,self.getinfo(item))
        else:
            prefix = self._s3path(path)
            for k in self._s3bukt.list(prefix=prefix):
                name = relpath(self._uns3path(k.name,prefix))
                if name != "":
                    if not isinstance(name,unicode):
                        name = name.decode("utf8")
                    if wildcard is not None:
                        if callable(wildcard):
                            if not wildcard(basename(name)):
                                continue
                        else:
                            if not fnmatch(basename(name),wildcard):
                                continue
                    yield (pathjoin(path,name),self._get_key_info(k,name))


    def walkfilesinfo(self,
              path="/",
              wildcard=None,
              dir_wildcard=None,
              search="breadth",
              ignore_errors=False ):
        if search != "breadth" or dir_wildcard is not None:
            args = (wildcard,dir_wildcard,search,ignore_errors)
            for item in super(S3FS,self).walkfiles(path,*args):
                yield (item,self.getinfo(item))
        else:
            prefix = self._s3path(path)
            for k in self._s3bukt.list(prefix=prefix):
                name = relpath(self._uns3path(k.name,prefix))
                if name != "":
                    if not isinstance(name,unicode):
                        name = name.decode("utf8")
                    if not k.name.endswith(self._separator):
                        if wildcard is not None:
                            if callable(wildcard):
                                if not wildcard(basename(name)):
                                    continue
                            else:
                                if not fnmatch(basename(name),wildcard):
                                    continue
                        yield (pathjoin(path,name),self._get_key_info(k,name))



def _eq_utf8(name1,name2):
    if isinstance(name1,unicode):
        name1 = name1.encode("utf8")
    if isinstance(name2,unicode):
        name2 = name2.encode("utf8")
    return name1 == name2

def _startswith_utf8(name1,name2):
    if isinstance(name1,unicode):
        name1 = name1.encode("utf8")
    if isinstance(name2,unicode):
        name2 = name2.encode("utf8")
    return name1.startswith(name2)

