"""
fs.s3fs
=======

FS subclass accessing files in Amazon S3

This module provides the class 'S3FS', which implements the FS filesystem
interface for objects stored in Amazon Simple Storage Service (S3).

"""

import time
import datetime
import tempfile
import stat as statinfo

import boto.s3.connection
from boto.s3.prefix import Prefix
from boto.exception import S3ResponseError

from fs.base import *
from fs.path import *
from fs.errors import *
from fs.remote import *


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

    class meta:
        PATH_MAX = None
        NAME_MAX = None

    def __init__(self, bucket, prefix="", aws_access_key=None, aws_secret_key=None, separator="/", thread_synchronize=True,key_sync_timeout=1):
        """Constructor for S3FS objects.

        S3FS objects require the name of the S3 bucket in which to store
        files, and can optionally be given a prefix under which the files
        shoud be stored.  The AWS public and private keys may be specified
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
        self._prefix = prefix
        self._tlocal = thread_local()
        super(S3FS, self).__init__(thread_synchronize=thread_synchronize)

    #  Make _s3conn and _s3bukt properties that are created on demand,
    #  since they cannot be stored during pickling.

    def _s3conn(self):
        try:
            return self._tlocal.s3conn
        except AttributeError:
            c = boto.s3.connection.S3Connection(*self._access_keys)
            self._tlocal.s3conn = c
            return c
    _s3conn = property(_s3conn)

    def _s3bukt(self):
        try:
            return self._tlocal.s3bukt
        except AttributeError:
            try:
                b = self._s3conn.get_bucket(self._bucket_name)
            except S3ResponseError, e:
                if "404 Not Found" not in str(e):
                    raise e
                b = self._s3conn.create_bucket(self._bucket_name)
            self._tlocal.s3bukt = b
            return b
    _s3bukt = property(_s3bukt)

    def __getstate__(self):
        state = super(S3FS,self).__getstate__()
        del state['_tlocal']
        return state

    def __setstate__(self,state):
        super(S3FS,self).__setstate__(state)
        self._tlocal = thread_local()

    def __str__(self):
        return '<S3FS: %s:%s>' % (self._bucket_name,self._prefix)

    __repr__ = __str__

    def _s3path(self,path):
        """Get the absolute path to a file stored in S3."""
        path = relpath(normpath(path))
        path = self._separator.join(iteratepath(path))
        s3path = self._prefix + path
        if s3path and s3path[-1] == self._separator:
            s3path = s3path[:-1]
        return s3path

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
                contents = tf
            key.set_contents_from_file(contents)
        return self._sync_key(key)

    def setcontents(self,path,contents):
        s3path = self._s3path(path)
        self._sync_set_contents(s3path,contents)

    def open(self,path,mode="r"):
        """Open the named file in the given mode.

        This method downloads the file contents into a local temporary file
        so that it can be worked on efficiently.  Any changes made to the
        file are only sent back to S3 when the file is flushed or closed.
        """
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
        return RemoteFileBuffer(self,path,mode,k)

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
            if k.name == s3path:
                return True
            # A directory
            if k.name == s3pathD:
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

    def listdir(self,path="./",wildcard=None,full=False,absolute=False,info=False,dirs_only=False,files_only=False):
        """List contents of a directory."""
        s3path = self._s3path(path) + self._separator
        if s3path == "/":
            s3path = ""
        i = len(s3path)
        keys = []
        isDir = False
        for k in self._s3bukt.list(prefix=s3path,delimiter=self._separator):
            if not isDir:
                isDir = True
            # Skip over the entry for the directory itself, if it exists
            if k.name[i:] != "":
                k.name = k.name[i:]
                keys.append(k)
        if not isDir:
            if s3path != self._prefix:
                if self.isfile(path):
                    raise ResourceInvalidError(path,msg="that's not a directory: %(path)s")
                raise ResourceNotFoundError(path)
        return self._listdir_helper(path,keys,wildcard,full,absolute,info,dirs_only,files_only)

    def _listdir_helper(self,path,keys,wildcard,full,absolute,info,dirs_only,files_only):
        """Modify listdir helper to avoid additional calls to the server."""
        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only can not both be True")
        if dirs_only:
            keys = [k for k in keys if k.name.endswith(self._separator)]
        elif files_only:
            keys = [k for k in keys if not k.name.endswith(self._separator)]

        for k in keys:
            if k.name.endswith(self._separator):
                k.name = k.name[:-1]
            if type(path) is not unicode:
                k.name = k.name.encode()

        if wildcard is not None:
            keys = [k for k in keys if fnmatch.fnmatch(k.name, wildcard)]

        if full:
            entries = [relpath(pathjoin(path, k.name)) for k in keys]
        elif absolute:
            entries = [abspath(pathjoin(path, k.name)) for k in keys]
        elif info:
            entries = [self._get_key_info(k) for k in keys]
        else:
            entries = [k.name for k in keys]
        return entries

    def makedir(self,path,recursive=False,allow_recreate=False):
        """Create a directory at the given path.

        The 'mode' argument is accepted for compatability with the standard
        FS interface, but is currently ignored.
        """
        s3path = self._s3path(path)
        s3pathD = s3path + self._separator
        if s3pathD == self._prefix:
            if allow_recreate:
                return
            raise DestinationExistsError(path, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
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
            if k.name == s3path:
                # It's already a file
                raise ResourceInvalidError(path, msg="Destination exists as a regular file: %(path)s")
            if k.name == s3pathD:
                # It's already a directory
                if allow_recreate:
                    return
                raise DestinationExistsError(path, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
        # Create parent if required
        if not parentExists:
            if recursive:
                self.makedir(dirname(path),recursive,allow_recreate)
            else:
                raise ParentDirectoryMissingError(path, msg="Parent directory does not exist: %(path)s")
        # Create an empty file representing the directory
        # TODO: is there some standard scheme for representing empty dirs?
        self._sync_set_contents(s3pathD,"")

    def remove(self,path):
        """Remove the file at the given path."""
        s3path = self._s3path(path)
        ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        for k in ks:
            if k.name == s3path:
                break
            if k.name.startswith(s3path + "/"):
                raise ResourceInvalidError(path,msg="that's not a file: %(path)s")
        else:
            raise ResourceNotFoundError(path)
        self._s3bukt.delete_key(s3path)
        k = self._s3bukt.get_key(s3path)
        while k:
            k = self._s3bukt.get_key(s3path)

    def removedir(self,path,recursive=False,force=False):
        """Remove the directory at the given path."""
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
            if k.name != s3path:
                if not force:
                    raise DirectoryNotEmptyError(path)
                self._s3bukt.delete_key(k.name)
        if not found:
            if self.isfile(path):
                raise ResourceInvalidError(path,msg="removedir() called on a regular file: %(path)s")
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
        self.move(src,dst)

    def getinfo(self,path):
        s3path = self._s3path(path)
        if path in ("","/"):
            k = Prefix(bucket=self._s3bukt,name="/")
        else:
            k = self._s3bukt.get_key(s3path)
            if k is None:
                k = self._s3bukt.get_key(s3path+"/")
                if k is None:
                    raise ResourceNotFoundError(path)
                k = Prefix(bucket=self._s3bukt,name=k.name)
        return self._get_key_info(k)

    def _get_key_info(self,key):
        info = {}
        info["name"] = basename(key.name)
        if isinstance(key,Prefix):
            info["st_mode"] = 0700 | statinfo.S_IFDIR
        else:
            info["st_mode"] =  0700 | statinfo.S_IFREG
        if hasattr(key,"size"):
            info['size'] = int(key.size)
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
            if k.name == s3path_dst:
                if not overwrite:
                    raise DestinationExistsError(dst)
                dstOK = True
                break
            # Check if it refers to a directory.  If so, we copy *into* it.
            # Since S3 lists in lexicographic order, subsequent iterations
            # of the loop will check for the existence of the new filename.
            if k.name == s3path_dstD:
                nm = basename(src)
                dst = pathjoin(dirname(dst),nm)
                s3path_dst = s3path_dstD + nm
                dstOK = True
        if not dstOK and not self.isdir(dirname(dst)):
            raise ParentDirectoryMissingError(dst,msg="Destination directory does not exist: %(path)s")
        # OK, now we can copy the file.
        s3path_src = self._s3path(src)
        try:
            self._s3bukt.copy_key(s3path_dst,self._bucket_name,s3path_src)
        except S3ResponseError, e:
            if "404 Not Found" in str(e):
                raise ResourceInvalidError(src, msg="Source is not a file: %(path)s")
            raise e
        else:
            k = self._s3bukt.get_key(s3path_dst)
            self._sync_key(k)

    def move(self,src,dst,overwrite=False,chunk_size=16384):
        """Move a file from one location to another."""
        self.copy(src,dst,overwrite=overwrite)
        self._s3bukt.delete_key(self._s3path(src))

    def get_total_size(self):
        """Get total size of all files in this FS."""
        return sum(k.size for k in self._s3bukt.list(prefix=self._prefix))
