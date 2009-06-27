"""

  fs.remote:  utilities for interfacing with remote filesystems

"""

import time
import copy

from fs.path import *
from fs.errors import *
from fs.wrapfs import WrapFS

try:
   from tempfile import SpooledTemporaryFile as TempFile
except ImportError:
   from tempfile import NamedTemporaryFile as TempFile


class RemoteFileBuffer(object):
    """File-like object providing buffer for local file operations.

    Instances of this class manage a local tempfile buffer corresponding
    to the contents of a remote file.  All reads and writes happen locally,
    with the content being copied to the remote file only on flush() or
    close().  Writes to the remote file are performed using the setcontents()
    method on the owning FS object.

    The intended use-case is for a remote filesystem (e.g. S3FS) to return
    instances of this class from its open() method, and to provide the
    file-uploading logic in its setcontents() method, as in the following
    pseudo-code:

        def open(self,path,mode="r"):
            rf = self._get_remote_file(path)
            return RemoteFileBuffer(self,path,mode,rf)

        def setcontents(self,path,file):
            self._put_remote_file(path,file)

    The current implementation reads the entire contents of the file into
    the buffer before returning.  Future implementations may pull data into
    the buffer on demand.
    """

    def __init__(self,fs,path,mode,rfile=None):
        """RemoteFileBuffer constructor.

        The owning filesystem, path and mode must be provided.  If the
        optional argument 'rfile' is provided, it must be a read()-able
        object containing the initial file contents.
        """
        self.file = TempFile()
        self.fs = fs
        self.path = path
        self.mode = mode
        self.closed = False
        if rfile is not None:
            data = rfile.read(1024*256)
            while data:
                self.file.write(data)
                data = rfile.read(1024*256)
            if "a" not in mode:
                self.file.seek(0)

    def __del__(self):
        if not self.closed:
            self.close()

    #  This is lifted straight from the stdlib's tempfile.py
    def __getattr__(self,name):
        file = self.__dict__['file']
        a = getattr(file, name)
        if not issubclass(type(a), type(0)):
            setattr(self, name, a)
        return a

    def __enter__(self):
        self.file.__enter__()
        return self

    def __exit__(self,exc,value,tb):
        self.close()
        return False

    def __iter__(self):
        return iter(self.file)

    def flush(self):
        self.file.flush()
        if "w" in self.mode or "a" in self.mode or "+" in self.mode:
            pos = self.file.tell()
            self.file.seek(0)
            self.fs.setcontents(self.path,self.file)
            self.file.seek(pos)

    def close(self):
        self.closed = True
        if "w" in self.mode or "a" in self.mode or "+" in self.mode:
            self.file.seek(0)
            self.fs.setcontents(self.path,self.file)
        self.file.close()



def cached(func):
    """Method decorator that caches results for CacheFS."""
    @wraps(func)
    def wrapper(self,path="",*args,**kwds):
        try:
            (success,result) = self._cache_get(path,func.__name__,args,kwds)
        except KeyError:
            try:
                res = func(self,path,*args,**kwds)
            except Exception, e:
                self._cache_set(path,func.__name__,args,kwds,(False,e))
                raise
            else:
                self._cache_set(path,func.__name__,args,kwds,(True,res))
                return copy.copy(res)
        else:
            if not success:
                raise result
            else:
                return copy.copy(result)
    return wrapper


class CacheFS(WrapFS):
    """Simple wrapper to cache meta-data of a remote filesystems.

    This FS wrapper implements a simplistic cache that can help speedup
    access to a remote filesystem.  File and directory meta-data is cached
    but the actual file contents are not.
    """

    def __init__(self,fs,timeout=1):
        """CacheFS constructor.

        The optional argument 'timeout' specifies the cache timeout in
        seconds.  The default timeout is 1 second.  To prevent cache
        entries from ever timing out, set it to None.
        """
        self.timeout = timeout
        self._cache = {"":{}}
        super(CacheFS,self).__init__(fs)

    def _path_cache(self,path):
        cache = self._cache
        for name in iteratepath(path):
            cache = cache.setdefault(name,{"":{}})
        return cache

    def _cache_get(self,path,func,args,kwds):
        now = time.time()
        cache = self._path_cache(path)
        key = (tuple(args),tuple(sorted(kwds.iteritems())))
        (t,v) = cache[""][func][key]
        if self.timeout is not None:
            if t < now - self.timeout:
                raise KeyError
        return v

    def _cache_set(self,path,func,args,kwds,v):
        t = time.time()
        cache = self._path_cache(path)
        key = (tuple(args),tuple(sorted(kwds.iteritems())))
        cache[""].setdefault(func,{})[key] = (t,v)

    def _uncache(self,path,added=False,removed=False,unmoved=False):
        cache = self._cache
        names = list(iteratepath(path))
        # If it's not the root dir, also clear some items for ancestors
        if names:
            # Clear cached 'getinfo' and 'getsize' for all ancestors 
            for name in names[:-1]:
                cache[""].pop("getinfo",None)
                cache[""].pop("getsize",None)
                cache = cache.get(name,None)
                if cache is None:
                    return 
            # Adjust cached 'listdir' for parent directory.
            # TODO: account for whether it was added, removed, or unmoved
            cache[""].pop("getinfo",None)
            cache[""].pop("getsize",None)
            cache[""].pop("listdir",None)
        # Clear all cached info for the path itself.
        cache[names[-1]] = {"":{}}

    @cached
    def exists(self,path):
        return super(CacheFS,self).exists(path)

    @cached
    def isdir(self,path):
        return super(CacheFS,self).isdir(path)

    @cached
    def isfile(self,path):
        return super(CacheFS,self).isfile(path)

    @cached
    def listdir(self,path="",**kwds):
        return super(CacheFS,self).listdir(path,**kwds)

    @cached
    def getinfo(self,path):
        return super(CacheFS,self).getinfo(path)

    @cached
    def getsize(self,path):
        return super(CacheFS,self).getsize(path)

    @cached
    def getxattr(self,path,name):
        return super(CacheFS,self).getxattr(path,name)

    @cached
    def listxattrs(self,path):
        return super(CacheFS,self).listxattrs(path)

    def open(self,path,mode="r"):
        f = super(CacheFS,self).open(path,mode)
        self._uncache(path,unmoved=True)
        return f

    def setcontents(self,path,contents):
        res = super(CacheFS,self).setcontents(path,contents)
        self._uncache(path,unmoved=True)
        return res

    def getcontents(self,path):
        res = super(CacheFS,self).getcontents(path)
        self._uncache(path,unmoved=True)
        return res

    def makedir(self,path,**kwds):
        super(CacheFS,self).makedir(path,**kwds)
        self._uncache(path,added=True)

    def remove(self,path):
        super(CacheFS,self).remove(path)
        self._uncache(path,removed=True)

    def removedir(self,path,**kwds):
        super(CacheFS,self).removedir(path,**kwds)
        self._uncache(path,removed=True)

    def rename(self,src,dst):
        super(CacheFS,self).rename(src,dst)
        self._uncache(src,removed=True)
        self._uncache(dst,added=True)

    def copy(self,src,dst,**kwds):
        super(CacheFS,self).copy(src,dst,**kwds)
        self._uncache(dst,added=True)

    def copydir(self,src,dst,**kwds):
        super(CacheFS,self).copydir(src,dst,**kwds)
        self._uncache(dst,added=True)

    def move(self,src,dst,**kwds):
        super(CacheFS,self).move(src,dst,**kwds)
        self._uncache(src,removed=True)
        self._uncache(dst,added=True)

    def movedir(self,src,dst,**kwds):
        super(CacheFS,self).movedir(src,dst,**kwds)
        self._uncache(src,removed=True)
        self._uncache(dst,added=True)

    def setxattr(self,path,name,value):
        self._uncache(path,unmoved=True)
        return super(CacheFS,self).setxattr(path,name,value)

    def delxattr(self,path,name):
        self._uncache(path,unmoved=True)
        return super(CacheFS,self).delxattr(path,name)

