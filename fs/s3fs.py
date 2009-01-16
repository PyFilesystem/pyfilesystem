"""

  fs.s3fs:  FS subclass accessing files in Amazon S3

This module provides the class 'S3FS', which implements the FS filesystem
interface for objects stored in Amazon Simple Storage Service (S3).

"""

import boto.s3.connection
from boto.exception import S3ResponseError

import time
import datetime
try:
   from tempfile import SpooledTemporaryFile as TempFile
except ImportError:
   from tempfile import NamedTemporaryFile as TempFile

from fs.base import *
from fs.helpers import *

    
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

    def __init__(self, bucket, prefix="", aws_access_key=None, aws_secret_key=None, separator="/", thread_syncronize=True,key_sync_timeout=1):
        """Constructor for S3FS objects.

        S3FS objects required the name of the S3 bucket in which to store
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
        self._separator = separator
        self._key_sync_timeout = key_sync_timeout
        self._s3conn = boto.s3.connection.S3Connection(aws_access_key,aws_secret_key)
        self._s3bukt = self._s3conn.create_bucket(bucket)
        # Normalise prefix to this form: path/to/files/
        while prefix.startswith(separator):
            prefix = prefix[1:]
        if not prefix.endswith(separator):
            prefix = prefix + separator
        self._prefix = prefix
        FS.__init__(self, thread_syncronize=thread_syncronize)

    def __str__(self):
        return '<S3FS: %s:%s>' % (self._bucket_name,self._prefix)

    __repr__ = __str__

    def _s3path(self,path):
        """Get the absolute path to a file stored in S3."""
        path = self._prefix + path
        path = self._separator.join(self._pathbits(path))
        return path

    def _pathbits(self,path):
        """Iterator over path components."""
        for bit in path.split("/"):
            if bit and bit != ".":
              yield bit

    def _sync_key(self,k):
        """Synchronise on contents of the given key.

        Since S3 only offers "eventual consistency" of data, it is possible
        to create a key but be unable to read it back straight away.  This
        method works around that limitation by polling the key until it reads
        back the expected by the given key.

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
            key.set_contents_from_file(contents)
        return self._sync_key(key)

    def open(self,path,mode="r"):
        """Open the named file in the given mode.

        This method downloads the file contents into a local temporary file
        so that it can be worked on efficiently.  Any changes made to the
        file are only sent back to S3 when the file is flushed or closed.
        """
        tf = TempFile()
        s3path = self._s3path(path)
        # Truncate the file if requested
        if "w" in mode:
            k = self._sync_set_contents(s3path,"")
        else:
            k = self._s3bukt.get_key(s3path)
        if k is None:
            # Create the file if it's missing
            if "w" not in mode and "a" not in mode:
                raise ResourceNotFoundError("NO_FILE",path)
            if not self.isdir(dirname(path)):
                raise OperationFailedError("OPEN_FAILED", path,msg="Parent directory does not exist")
            k = self._sync_set_contents(s3path,"")
        else:
            # Get the file contents into the tempfile.
            if "r" in mode or "+" in mode or "a" in mode:
                k.get_contents_to_file(tf)
                if "a" not in mode:
                    tf.seek(0)
        # Upload the tempfile when it is flushed or closed
        if "w" in mode or "a" in mode or "+" in mode:
            oldflush = tf.flush
            oldclose = tf.close
            def newflush():
                oldflush()
                pos = tf.tell()
                tf.seek(0)
                self._sync_set_contents(k,tf)
                tf.seek(pos)
            def newclose():
                oldflush()
                tf.seek(0)
                self._sync_set_contents(k,tf)
                oldclose()
            tf.close = newclose
            tf.flush = newflush
        return tf

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
        if s3path == self._prefix:
            return True
        # Use a list request so that we return true if there are any files
        # in that directory.  This avoids requiring a special file for the
        # the directory itself, which other tools may not create.
        ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        for k in ks:
            return True
        return False

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

    def listdir(self,path="./",wildcard=None,full=False,absolute=False,hidden=True,dirs_only=False,files_only=False):
        """List contents of a directory."""
        s3path = self._s3path(path) + self._separator
        if s3path == "/":
            s3path = ""
        i = len(s3path)
        ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        paths = []
        isDir = False
        for k in ks:
            if not isDir:
                isDir = True
            nm = k.name[i:]
            # Skip over the entry for the directory itself, if it exists
            if nm != "":
                if type(path) is not unicode:
                    nm = nm.encode()
                paths.append(nm)
        if not isDir:
            if s3path != self._prefix:
                print "NOT A DIR:", s3path
                raise OperationFailedError("LISTDIR_FAILED",path)
        return self._listdir_helper(path,paths,wildcard,full,absolute,hidden,dirs_only,files_only)

    def _listdir_helper(self,path,paths,wildcard,full,absolute,hidden,dirs_only,files_only):
        """Modify listdir helper to avoid additional calls to the server."""
        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only can not both be True")
        dirs = [p[:-1] for p in paths if p.endswith(self._separator)]
        files = [p for p in paths if not p.endswith(self._separator)]

        if dirs_only:
            paths = dirs
        elif files_only:
            paths = files
        else:
            paths = dirs + files

        if wildcard is not None:
            match = fnmatch.fnmatch
            paths = [p for p in paths if match(p, wildcard)]

        if not hidden:
            paths = [p for p in paths if not self.ishidden(p)]

        if full:
            paths = [pathjoin(path, p) for p in paths]
        elif absolute:
            paths = [self._abspath(pathjoin(path, p)) for p in paths]

        return paths
        
    def makedir(self,path,mode=0777,recursive=False,allow_recreate=False):
        """Create a directory at the given path.

        The 'mode' argument is accepted for compatability with the standard
        FS interface, but is currently ignored.
        """
        s3path = self._s3path(path) 
        s3pathD = s3path + self._separator
        if s3pathD == self._prefix:
            if allow_recreate:
                return
            raise OperationFailedError("MAKEDIR_FAILED", path, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
        s3pathP = self._s3path(dirname(path[:-1])) + self._separator
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
                raise OperationFailedError("MAKEDIR_FAILED", path, msg="Can not create a directory that already exists: %(path)s")
            if k.name == s3pathD:
                # It's already a directory
                if allow_recreate:
                    return
                raise OperationFailedError("MAKEDIR_FAILED", path, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
        # Create parent if required
        if not parentExists:
            if recursive:
                self.makedir(dirname(path[:-1]),mode,recursive,allow_recreate)
            else:
                raise OperationFailedError("MAKEDIR_FAILED",path, msg="Parent directory does not exist: %(path)s")
        # Create an empty file representing the directory
        # TODO: is there some standard scheme for representing empty dirs?
        self._sync_set_contents(s3pathD,"")

    def remove(self,path):
        """Remove the file at the given path."""
        # TODO: This will fail silently if the key doesn't exist
        s3path = self._s3path(path)
        self._s3bukt.delete_key(s3path)
        k = self._s3bukt.get_key(s3path)
        while k:
            k = self._s3bukt.get_key(s3path)

    def removedir(self,path,recursive=False):
        """Remove the directory at the given path."""
        s3path = self._s3path(path) + self._separator
        ks = self._s3bukt.list(prefix=s3path,delimiter=self._separator)
        # Fail if the directory is not empty
        for k in ks:
            if k.name != s3path:
                raise OperationFailedError("REMOVEDIR_FAILED",path)
        self._s3bukt.delete_key(s3path)
        if recursive:
            pdir = dirname(path)
            try:
                self.removedir(pdir,True)
            except OperationFailedError:
                pass
        
    def rename(self,src,dst):
        """Rename the file at 'src' to 'dst'."""
        if not issamedir(src,dst):
            raise ValueError("Destination path must be in the same directory (use the 'move' method for moving to a different directory)")
        # Actually, in S3 'rename' is exactly the same as 'move'
        self.move(src,dst)

    def getinfo(self,path):
        s3path = self._s3path(path)
        k = self._s3bukt.get_key(s3path)
        info = {}
        info['size'] = int(k.size)
        fmt = "%a, %d %b %Y %H:%M:%S %Z"
        info['modified_time'] = datetime.datetime.strptime(k.last_modified,fmt)
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
                    raise OperationFailedError("COPYFILE_FAILED",src,dst,msg="Destination file exists: %(path2)s")
                dstOK = True
                break
            # Check if it refers to a directory.  If so, we copy *into* it.
            # Since S3 lists in lexicographic order, subsequence iterations
            # of the loop will check for the existence of the new filename.
            if k.name == s3path_dstD:
                nm = resourcename(src)
                dst = pathjoin(dirname(dst),nm)
                s3path_dst = s3path_dstD + nm
                dstOK = True
        if not dstOK and not self.isdir(dirname(dst)):
            raise OperationFailedError("COPYFILE_FAILED",src,dst,msg="Destination directory does not exist")
        # OK, now we can copy the file.
        s3path_src = self._s3path(src)
        try:
            self._s3bukt.copy_key(s3path_dst,self._bucket_name,s3path_src)
        except S3ResponseError, e:
            if "404 Not Found" in str(e):
                raise ResourceInvalid("WRONG_TYPE", src, msg="Source is not a file: %(path)s")
            raise e
        else:
            k = self._s3bukt.get_key(s3path_dst)
            self._sync_key(k)

    def move(self,src,dst,chunk_size=16384):
        """Move a file from one location to another."""
        self.copy(src,dst)
        self._s3bukt.delete_key(self._s3path(src))

