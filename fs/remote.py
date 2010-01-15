"""

fs.remote
=========

Utilities for interfacing with remote filesystems


This module provides reusable utility functions that can be used to construct
FS subclasses interfacing with a remote filesystem.  These include:

  * RemoteFileBuffer:  a file-like object that locally buffers the contents of
                       a remote file, writing them back on flush() or close().

  * ConnectionManagerFS:  a WrapFS subclass that tracks the connection state
                          of a remote FS, and allows client code to wait for
                          a connection to be re-established.

   * CacheFS:  a WrapFS subclass that caces file and directory meta-data in
               memory, to speed access to a remote FS.

"""

import time
import copy

from fs.base import FS, threading
from fs.wrapfs import WrapFS, wrap_fs_methods
from fs.wrapfs.lazyfs import LazyFS
from fs.path import *
from fs.errors import *

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
    pseudo-code::

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
        object or a string containing the initial file contents.
        """
        self.file = TempFile()
        self.fs = fs
        self.path = path
        self.mode = mode
        self.closed = False
        self._flushed = False
        if getattr(fs,"_lock",None) is not None:
            self._lock = fs._lock.__class__()
        else:
            self._lock = threading.RLock()
        if "r" in mode or "+" in mode or "a" in mode:
            if rfile is not None:
                if hasattr(rfile,"read"):
                    data = rfile.read(1024*256)
                    while data:
                        self.file.write(data)
                        data = rfile.read(1024*256)
                else:
                    self.file.write(str(rfile))
                if "a" not in mode:
                    self.file.seek(0)
        
    def __del__(self):
        if not self.closed:
            self.close()

    def __getattr__(self,name):
        file = self.__dict__['file']
        a = getattr(file, name)
        if not callable(a):
            return a
        @wraps(a)
        def call_with_lock(*args,**kwds):
            self._lock.acquire()
            try:
                if "write" in name:
                    self._flushed = False
                return a(*args,**kwds)
            finally:
                self._lock.release()
        setattr(self, name, call_with_lock)
        return call_with_lock

    def __enter__(self):
        self.file.__enter__()
        return self

    def __exit__(self,exc,value,tb):
        self.close()
        return False

    def __iter__(self):
        return iter(self.file)

    def truncate(self,size=None):
        self._lock.acquire()
        try:
            self.file.truncate(size)
            self.flush()
        finally:
            self._lock.release()

    def flush(self):
        self._lock.acquire()
        try:
            self.file.flush()
            if "w" in self.mode or "a" in self.mode or "+" in self.mode:
                if not self._flushed:
                    pos = self.file.tell()
                    self.file.seek(0)
                    self.fs.setcontents(self.path,self.file)
                    self.file.seek(pos)
                    self._flushed = True
        finally:
            self._lock.release()

    def close(self):
        self._lock.acquire()
        try:
            if not self.closed:
                self.closed = True
                if "w" in self.mode or "a" in self.mode or "+" in self.mode:
                    if not self._flushed:
                        self.file.seek(0)
                        self.file.seek(0)
                        self.fs.setcontents(self.path,self.file)
                self.file.close()
        finally:
            self._lock.release()


class ConnectionManagerFS(LazyFS):
    """FS wrapper providing simple connection management of a remote FS.

    The ConnectionManagerFS class is designed to wrap a remote FS object
    and provide some convenience methods for dealing with its remote
    connection state.

    The boolean attribute 'connected' indicates whether the remote fileystem
    has an active connection, and is initially True.  If any of the remote
    filesystem methods raises a RemoteConnectionError, 'connected' will
    switch to False and remain so until a successful remote method call.

    Application code can use the method 'wait_for_connection' to block
    until the connection is re-established.  Currently this reconnection
    is checked by a simple polling loop; eventually more sophisticated
    operating-system integration may be added.

    Since some remote FS classes can raise RemoteConnectionError during
    initialisation, this class makes use of lazy initialization. The
    remote FS can be specified as an FS instance, an FS subclass, or a
    (class,args) or (class,args,kwds) tuple. For example::

        >>> fs = ConnectionManagerFS(MyRemoteFS("http://www.example.com/"))
        Traceback (most recent call last):
            ...
        RemoteConnectionError: couldn't connect to "http://www.example.com/"
        >>> fs = ConnectionManagerFS((MyRemoteFS,["http://www.example.com/"]))
        >>> fs.connected
        False
        >>>

    """

    poll_interval = 1

    def __init__(self,wrapped_fs,poll_interval=None,connected=True):
        super(ConnectionManagerFS,self).__init__(wrapped_fs)
        if poll_interval is not None:
            self.poll_interval = poll_interval
        self._connection_cond = threading.Condition()
        self._poll_thread = None
        self._poll_sleeper = threading.Event()
        self.connected = connected

    def setcontents(self,path,data):
        self.wrapped_fs.setcontents(path,data)

    def __getstate__(self):
        state = super(ConnectionManagerFS,self).__getstate__()
        del state["_connection_cond"]
        del state["_poll_sleeper"]
        state["_poll_thread"] = None
        return state

    def __setstate__(self,state):
        super(ConnectionManagerFS,self).__setstate__(state)
        self._connection_cond = threading.Condition()
        self._poll_sleeper = threading.Event()
        
    def wait_for_connection(self,timeout=None):
        self._connection_cond.acquire()
        try:
            if not self.connected:
                if not self._poll_thread:
                    target = self._poll_connection
                    self._poll_thread = threading.Thread(target=target)
                    self._poll_thread.start()
                self._connection_cond.wait(timeout)
        finally:
            self._connection_cond.release()

    def _poll_connection(self):
        while not self.connected:
            try:
                self.wrapped_fs.isdir("")
            except RemoteConnectionError:
                self._poll_sleeper.wait(self.poll_interval)
                self._poll_sleeper.clear()
            except FSError:
                break
            else:
                break
        self._connection_cond.acquire()
        try:
            self.connected = True
            self._poll_thread = None
            self._connection_cond.notifyAll()
        finally:
            self._connection_cond.release()

    def close(self):
        if not self.closed:
            try:
                super(ConnectionManagerFS,self).close()
            except (RemoteConnectionError,):
                pass
            if self._poll_thread:
                self.connected = True
                self._poll_sleeper.set()
                self._poll_thread.join()
                self._poll_thread = None

def _ConnectionManagerFS_method_wrapper(func):
    """Method wrapper for ConnectionManagerFS.

    This method wrapper keeps an eye out for RemoteConnectionErrors and
    adjusts self.connected accordingly.
    """
    @wraps(func)
    def wrapper(self,*args,**kwds):
        try:
            result = func(self,*args,**kwds)
        except RemoteConnectionError:
            self.connected = False
            raise
        except FSError:
            self.connected = True
            raise
        else:
            self.connected = True
            return result
    return wrapper
 
wrap_fs_methods(_ConnectionManagerFS_method_wrapper,ConnectionManagerFS)


def _cached_method(func):
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

    This FS wrapper implements a simplistic cache that can help speed up
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

    @_cached_method
    def exists(self,path):
        return super(CacheFS,self).exists(path)

    @_cached_method
    def isdir(self,path):
        return super(CacheFS,self).isdir(path)

    @_cached_method
    def isfile(self,path):
        return super(CacheFS,self).isfile(path)

    @_cached_method
    def listdir(self,path="",**kwds):
        return super(CacheFS,self).listdir(path,**kwds)

    @_cached_method
    def getinfo(self,path):
        return super(CacheFS,self).getinfo(path)

    @_cached_method
    def getsize(self,path):
        return super(CacheFS,self).getsize(path)

    @_cached_method
    def getxattr(self,path,name,default=None):
        return super(CacheFS,self).getxattr(path,name,default)

    @_cached_method
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

