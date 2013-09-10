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

  * CacheFS:  a WrapFS subclass that caches file and directory meta-data in
              memory, to speed access to a remote FS.

"""

from __future__ import with_statement

import time
import stat as statinfo
from errno import EINVAL

import fs.utils
from fs.base import threading, FS
from fs.wrapfs import WrapFS, wrap_fs_methods
from fs.wrapfs.lazyfs import LazyFS
from fs.path import *
from fs.errors import *
from fs.local_functools import wraps
from fs.filelike import StringIO, SpooledTemporaryFile, FileWrapper
from fs import SEEK_SET, SEEK_CUR, SEEK_END


_SENTINAL = object()

from six import PY3, b


class RemoteFileBuffer(FileWrapper):
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

    The contents of the remote file are read into the buffer on-demand.
    """

    max_size_in_memory = 1024 * 8

    def __init__(self, fs, path, mode, rfile=None, write_on_flush=True):
        """RemoteFileBuffer constructor.

        The owning filesystem, path and mode must be provided.  If the
        optional argument 'rfile' is provided, it must be a read()-able
        object or a string containing the initial file contents.
        """
        wrapped_file = SpooledTemporaryFile(max_size=self.max_size_in_memory)
        self.fs = fs
        self.path = path
        self.write_on_flush = write_on_flush
        self._changed = False
        self._readlen = 0  # How many bytes already loaded from rfile
        self._rfile = None  # Reference to remote file object
        self._eof = False  # Reached end of rfile?
        if getattr(fs, "_lock", None) is not None:
            self._lock = fs._lock.__class__()
        else:
            self._lock = threading.RLock()

        if "r" in mode or "+" in mode or "a" in mode:
            if rfile is None:
                # File was just created, force to write anything
                self._changed = True
                self._eof = True

            if not hasattr(rfile, "read"):
                #rfile = StringIO(unicode(rfile))
                rfile = StringIO(rfile)

            self._rfile = rfile
        else:
            # Do not use remote file object
            self._eof = True
            self._rfile = None
            self._changed = True
            if rfile is not None and hasattr(rfile,"close"):
                rfile.close()
        super(RemoteFileBuffer,self).__init__(wrapped_file,mode)
        # FIXME: What if mode with position on eof?
        if "a" in mode:
            # Not good enough...
            self.seek(0, SEEK_END)

    def __del__(self):
        #  Don't try to close a partially-constructed file
        if "_lock" in self.__dict__:
            if not self.closed:
                try:
                    self.close()
                except FSError:
                    pass

    def _write(self,data,flushing=False):
        with self._lock:
            #  Do we need to discard info from the buffer?
            toread = len(data) - (self._readlen - self.wrapped_file.tell())
            if toread > 0:
                if not self._eof:
                    self._fillbuffer(toread)
                else:
                    self._readlen += toread
            self._changed = True
            self.wrapped_file.write(data)

    def _read_remote(self, length=None):
        """Read data from the remote file into the local buffer."""
        chunklen = 1024 * 256
        bytes_read = 0
        while True:
            toread = chunklen
            if length is not None and length - bytes_read < chunklen:
                toread = length - bytes_read
            if not toread:
                break

            data = self._rfile.read(toread)
            datalen = len(data)
            if not datalen:
                self._eof = True
                break

            bytes_read += datalen
            self.wrapped_file.write(data)

            if datalen < toread:
                # We reached EOF,
                # no more reads needed
                self._eof = True
                break

        if self._eof and self._rfile is not None:
            self._rfile.close()
        self._readlen += bytes_read

    def _fillbuffer(self, length=None):
        """Fill the local buffer, leaving file position unchanged.

        This method is used for on-demand loading of data from the remote file
        into the buffer.  It reads 'length' bytes from rfile and writes them
        into the buffer, seeking back to the original file position.
        """
        curpos = self.wrapped_file.tell()
        if length == None:
            if not self._eof:
                # Read all data and we didn't reached EOF
                # Merge endpos - tell + bytes from rfile
                self.wrapped_file.seek(0, SEEK_END)
                self._read_remote()
                self._eof = True
                self.wrapped_file.seek(curpos)

        elif not self._eof:
            if curpos + length > self._readlen:
                # Read all data and we didn't reached EOF
                # Load endpos - tell() + len bytes from rfile
                toload = length - (self._readlen - curpos)
                self.wrapped_file.seek(0, SEEK_END)
                self._read_remote(toload)
                self.wrapped_file.seek(curpos)

    def _read(self, length=None):
        if length is not None and length < 0:
            length = None
        with self._lock:
            self._fillbuffer(length)
            data = self.wrapped_file.read(length if length != None else -1)
            if not data:
                data = None
            return data

    def _seek(self,offset,whence=SEEK_SET):
        with self._lock:
            if not self._eof:
                # Count absolute position of seeking
                if whence == SEEK_SET:
                    abspos = offset
                elif whence == SEEK_CUR:
                    abspos =  offset + self.wrapped_file.tell()
                elif whence == SEEK_END:
                    abspos = None
                else:
                    raise IOError(EINVAL, 'Invalid whence')

                if abspos != None:
                    toread = abspos - self._readlen
                    if toread > 0:
                        self.wrapped_file.seek(self._readlen)
                        self._fillbuffer(toread)
                else:
                    self.wrapped_file.seek(self._readlen)
                    self._fillbuffer()

            self.wrapped_file.seek(offset, whence)

    def _truncate(self,size):
        with self._lock:
            if not self._eof and self._readlen < size:
                # Read the rest of file
                self._fillbuffer(size - self._readlen)
                # Lock rfile
                self._eof = True
            elif self._readlen >= size:
                # Crop rfile metadata
                self._readlen = size if size != None else 0
                # Lock rfile
                self._eof = True

            self.wrapped_file.truncate(size)
            self._changed = True

            self.flush()
            if self._rfile is not None:
                self._rfile.close()

    def flush(self):
        with self._lock:
            self.wrapped_file.flush()
            if self.write_on_flush:
                self._setcontents()

    def _setcontents(self):
        if not self._changed:
            # Nothing changed, no need to write data back
            return

        # If not all data loaded, load until eof
        if not self._eof:
            self._fillbuffer()

        if "w" in self.mode or "a" in self.mode or "+" in self.mode:
            pos = self.wrapped_file.tell()
            self.wrapped_file.seek(0)
            self.fs.setcontents(self.path, self.wrapped_file)
            self.wrapped_file.seek(pos)

    def close(self):
        with self._lock:
            if not self.closed:
                self._setcontents()
                if self._rfile is not None:
                    self._rfile.close()
                super(RemoteFileBuffer,self).close()


class ConnectionManagerFS(LazyFS):
    """FS wrapper providing simple connection management of a remote FS.

    The ConnectionManagerFS class is designed to wrap a remote FS object
    and provide some convenience methods for dealing with its remote
    connection state.

    The boolean attribute 'connected' indicates whether the remote filesystem
    has an active connection, and is initially True.  If any of the remote
    filesystem methods raises a RemoteConnectionError, 'connected' will
    switch to False and remain so until a successful remote method call.

    Application code can use the method 'wait_for_connection' to block
    until the connection is re-established.  Currently this reconnection
    is checked by a simple polling loop; eventually more sophisticated
    operating-system integration may be added.

    Since some remote FS classes can raise RemoteConnectionError during
    initialization, this class makes use of lazy initialization. The
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

    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=64*1024):
        return self.wrapped_fs.setcontents(path, data, encoding=encoding, errors=errors, chunk_size=chunk_size)

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

    def wait_for_connection(self,timeout=None,force_wait=False):
        self._connection_cond.acquire()
        try:
            if force_wait:
                self.connected = False
            if not self.connected:
                if not self._poll_thread:
                    target = self._poll_connection
                    self._poll_thread = threading.Thread(target=target)
                    self._poll_thread.daemon = True
                    self._poll_thread.start()
                self._connection_cond.wait(timeout)
        finally:
            self._connection_cond.release()

    def _poll_connection(self):
        while not self.connected and not self.closed:
            try:
                self.wrapped_fs.getinfo("/")
            except RemoteConnectionError:
                self._poll_sleeper.wait(self.poll_interval)
                self._poll_sleeper.clear()
            except FSError:
                break
            else:
                break
        self._connection_cond.acquire()
        try:
            if not self.closed:
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


class CachedInfo(object):
    """Info objects stored in cache for CacheFS."""
    __slots__ = ("timestamp","info","has_full_info","has_full_children")
    def __init__(self,info={},has_full_info=True,has_full_children=False):
        self.timestamp = time.time()
        self.info = info
        self.has_full_info = has_full_info
        self.has_full_children = has_full_children
    def clone(self):
        new_ci = self.__class__()
        new_ci.update_from(self)
        return new_ci
    def update_from(self,other):
        self.timestamp = other.timestamp
        self.info = other.info
        self.has_full_info = other.has_full_info
        self.has_full_children = other.has_full_children
    @classmethod
    def new_file_stub(cls):
        info = {"info" : 0700 | statinfo.S_IFREG}
        return cls(info,has_full_info=False)
    @classmethod
    def new_dir_stub(cls):
        info = {"info" : 0700 | statinfo.S_IFDIR}
        return cls(info,has_full_info=False)


class CacheFSMixin(FS):
    """Simple FS mixin to cache meta-data of a remote filesystems.

    This FS mixin implements a simplistic cache that can help speed up
    access to a remote filesystem.  File and directory meta-data is cached
    but the actual file contents are not.

    If you want to add caching to an existing FS object, use the CacheFS
    class instead; it's an easy-to-use wrapper rather than a mixin.
    This mixin class is provided for FS implementors who want to use
    caching internally in their own classes.

    FYI, the implementation of CacheFS is this:

        class CacheFS(CacheFSMixin,WrapFS):
            pass

    """

    def __init__(self,*args,**kwds):
        """CacheFSMixin constructor.

        The optional keyword argument 'cache_timeout' specifies the cache
        timeout in seconds.  The default timeout is 1 second.  To prevent
        cache entries from ever timing out, set it to None.

        The optional keyword argument 'max_cache_size' specifies the maximum
        number of entries to keep in the cache.  To allow the cache to grow
        without bound, set it to None.  The default is 1000.
        """
        self.cache_timeout = kwds.pop("cache_timeout",1)
        self.max_cache_size = kwds.pop("max_cache_size",1000)
        self.__cache = PathMap()
        self.__cache_size = 0
        self.__cache_lock = threading.RLock()
        super(CacheFSMixin,self).__init__(*args,**kwds)

    def clear_cache(self,path=""):
        with self.__cache_lock:
            self.__cache.clear(path)
        try:
            scc = super(CacheFSMixin,self).clear_cache
        except AttributeError:
            pass
        else:
            scc()

    def __getstate__(self):
        state = super(CacheFSMixin,self).__getstate__()
        state.pop("_CacheFSMixin__cache",None)
        state.pop("_CacheFSMixin__cache_size",None)
        state.pop("_CacheFSMixin__cache_lock",None)
        return state

    def __setstate__(self,state):
        super(CacheFSMixin,self).__setstate__(state)
        self.__cache = PathMap()
        self.__cache_size = 0
        self.__cache_lock = threading.RLock()

    def __get_cached_info(self,path,default=_SENTINAL):
        try:
            info = self.__cache[path]
            if self.cache_timeout is not None:
                now = time.time()
                if info.timestamp < (now - self.cache_timeout):
                    with self.__cache_lock:
                        self.__expire_from_cache(path)
                        raise KeyError
            return info
        except KeyError:
            if default is not _SENTINAL:
                return default
            raise

    def __set_cached_info(self,path,new_ci,old_ci=None):
        was_room = True
        with self.__cache_lock:
            #  Free up some room in the cache
            if self.max_cache_size is not None and old_ci is None:
                while self.__cache_size >= self.max_cache_size:
                    try:
                        to_del = iter(self.__cache).next()
                    except StopIteration:
                        break
                    else:
                        was_room = False
                        self.__expire_from_cache(to_del)
            #  Atomically add to the cache.
            #  If there's a race, newest information wins
            ci = self.__cache.setdefault(path,new_ci)
            if ci is new_ci:
                self.__cache_size += 1
            else:
                if old_ci is None or ci is old_ci:
                    if ci.timestamp < new_ci.timestamp:
                        ci.update_from(new_ci)
        return was_room

    def __expire_from_cache(self,path):
        del self.__cache[path]
        self.__cache_size -= 1
        for ancestor in recursepath(path):
            try:
                self.__cache[ancestor].has_full_children = False
            except KeyError:
                pass

    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        #  Try to validate the entry using the cached info
        try:
            ci = self.__get_cached_info(path)
        except KeyError:
            if path in ("", "/"):
                raise ResourceInvalidError(path)
            try:
                ppath = dirname(path)
                pci = self.__get_cached_info(ppath)
            except KeyError:
                pass
            else:
                if not fs.utils.isdir(super(CacheFSMixin, self), ppath, pci.info):
                    raise ResourceInvalidError(path)
                if pci.has_full_children:
                    raise ResourceNotFoundError(path)
        else:
            if not fs.utils.isfile(super(CacheFSMixin, self), path, ci.info):
                raise ResourceInvalidError(path)
        f = super(CacheFSMixin, self).open(path, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, line_buffering=line_buffering, **kwargs)
        if "w" in mode or "a" in mode or "+" in mode:
            with self.__cache_lock:
                self.__cache.clear(path)
            f = self._CacheInvalidatingFile(self, path, f, mode)
        return f

    class _CacheInvalidatingFile(FileWrapper):
        def __init__(self, owner, path, wrapped_file, mode=None):
            self.path = path
            sup = super(CacheFSMixin._CacheInvalidatingFile, self)
            sup.__init__(wrapped_file, mode)
            self.owner = owner
        def _write(self, string, flushing=False):
            with self.owner._CacheFSMixin__cache_lock:
                self.owner._CacheFSMixin__cache.clear(self.path)
            sup = super(CacheFSMixin._CacheInvalidatingFile, self)
            return sup._write(string, flushing=flushing)
        def _truncate(self, size):
            with self.owner._CacheFSMixin__cache_lock:
                self.owner._CacheFSMixin__cache.clear(self.path)
            sup = super(CacheFSMixin._CacheInvalidatingFile, self)
            return sup._truncate(size)

    def exists(self, path):
        try:
            self.getinfo(path)
        except ResourceNotFoundError:
            return False
        else:
            return True

    def isdir(self, path):
        try:
            self.__cache.iternames(path).next()
            return True
        except StopIteration:
            pass
        except RuntimeError:
            pass
        try:
            info = self.getinfo(path)
        except ResourceNotFoundError:
            return False
        else:
            return fs.utils.isdir(super(CacheFSMixin, self), path, info)

    def isfile(self, path):
        try:
            self.__cache.iternames(path).next()
            return False
        except StopIteration:
            pass
        except RuntimeError:
            pass
        try:
            info = self.getinfo(path)
        except ResourceNotFoundError:
            return False
        else:
            return fs.utils.isfile(super(CacheFSMixin, self), path, info)

    def getinfo(self, path):
        try:
            ci = self.__get_cached_info(path)
            if not ci.has_full_info:
                raise KeyError
            info = ci.info
        except KeyError:
            info = super(CacheFSMixin, self).getinfo(path)
            self.__set_cached_info(path, CachedInfo(info))
        return info

    def listdir(self,path="",*args,**kwds):
        return list(nm for (nm, _info) in self.listdirinfo(path,*args,**kwds))

    def ilistdir(self,path="",*args,**kwds):
        for (nm, _info) in self.ilistdirinfo(path,*args,**kwds):
            yield nm

    def listdirinfo(self,path="",*args,**kwds):
        items = super(CacheFSMixin,self).listdirinfo(path,*args,**kwds)
        with self.__cache_lock:
            names = set()
            for (nm,info) in items:
                names.add(basename(nm))
                cpath = pathjoin(path,basename(nm))
                ci = CachedInfo(info)
                self.__set_cached_info(cpath,ci)
            to_del = []
            for nm in self.__cache.names(path):
                if nm not in names:
                    to_del.append(nm)
            for nm in to_del:
                self.__cache.clear(pathjoin(path,nm))
            #try:
            #    pci = self.__cache[path]
            #except KeyError:
            #    pci = CachedInfo.new_dir_stub()
            #    self.__cache[path] = pci
            #pci.has_full_children = True
        return items

    def ilistdirinfo(self,path="",*args,**kwds):
        items = super(CacheFSMixin,self).ilistdirinfo(path,*args,**kwds)
        for (nm,info) in items:
            cpath = pathjoin(path,basename(nm))
            ci = CachedInfo(info)
            self.__set_cached_info(cpath,ci)
            yield (nm,info)

    def getsize(self,path):
        return self.getinfo(path)["size"]

    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=64*1024):
        supsc = super(CacheFSMixin, self).setcontents
        res = supsc(path, data, encoding=None, errors=None, chunk_size=chunk_size)
        with self.__cache_lock:
            self.__cache.clear(path)
            self.__cache[path] = CachedInfo.new_file_stub()
        return res

    def createfile(self, path, wipe=False):
        super(CacheFSMixin,self).createfile(path, wipe=wipe)
        with self.__cache_lock:
            self.__cache.clear(path)
            self.__cache[path] = CachedInfo.new_file_stub()

    def makedir(self,path,*args,**kwds):
        super(CacheFSMixin,self).makedir(path,*args,**kwds)
        with self.__cache_lock:
            self.__cache.clear(path)
            self.__cache[path] = CachedInfo.new_dir_stub()

    def remove(self,path):
        super(CacheFSMixin,self).remove(path)
        with self.__cache_lock:
            self.__cache.clear(path)

    def removedir(self,path,**kwds):
        super(CacheFSMixin,self).removedir(path,**kwds)
        with self.__cache_lock:
            self.__cache.clear(path)

    def rename(self,src,dst):
        super(CacheFSMixin,self).rename(src,dst)
        with self.__cache_lock:
            for (subpath,ci) in self.__cache.items(src):
                self.__cache[pathjoin(dst,subpath)] = ci.clone()
            self.__cache.clear(src)

    def copy(self,src,dst,**kwds):
        super(CacheFSMixin,self).copy(src,dst,**kwds)
        with self.__cache_lock:
            for (subpath,ci) in self.__cache.items(src):
                self.__cache[pathjoin(dst,subpath)] = ci.clone()

    def copydir(self,src,dst,**kwds):
        super(CacheFSMixin,self).copydir(src,dst,**kwds)
        with self.__cache_lock:
            for (subpath,ci) in self.__cache.items(src):
                self.__cache[pathjoin(dst,subpath)] = ci.clone()

    def move(self,src,dst,**kwds):
        super(CacheFSMixin,self).move(src,dst,**kwds)
        with self.__cache_lock:
            for (subpath,ci) in self.__cache.items(src):
                self.__cache[pathjoin(dst,subpath)] = ci.clone()
            self.__cache.clear(src)

    def movedir(self,src,dst,**kwds):
        super(CacheFSMixin,self).movedir(src,dst,**kwds)
        with self.__cache_lock:
            for (subpath,ci) in self.__cache.items(src):
                self.__cache[pathjoin(dst,subpath)] = ci.clone()
            self.__cache.clear(src)

    def settimes(self,path,*args,**kwds):
        super(CacheFSMixin,self).settimes(path,*args,**kwds)
        with self.__cache_lock:
            self.__cache.pop(path,None)


class CacheFS(CacheFSMixin,WrapFS):
    """Simple FS wrapper to cache meta-data of a remote filesystems.

    This FS mixin implements a simplistic cache that can help speed up
    access to a remote filesystem.  File and directory meta-data is cached
    but the actual file contents are not.
    """
    pass


