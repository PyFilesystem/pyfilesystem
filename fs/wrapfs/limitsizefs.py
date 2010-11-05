"""
fs.wrapfs.limitsizefs
=====================

An FS wrapper class for limiting the size of the underlying FS.

This module provides the class LimitSizeFS, an FS wrapper that can limit the
total size of files stored in the wrapped FS.

"""
# for Python2.5 compatibility
from __future__ import with_statement
from fs.errors import *
from fs.path import *
from fs.base import FS, threading, synchronize
from fs.wrapfs import WrapFS


class LimitSizeFS(WrapFS):
    """FS wrapper class to limit total size of files stored."""

    def __init__(self, fs, max_size):
        super(LimitSizeFS,self).__init__(fs)
        self.max_size = max_size
        self.cur_size = sum(self.getsize(f) for f in self.walkfiles())
        self._size_lock = threading.Lock()
        self._file_sizes = {}

    def _decr_size(self, decr):
        with self._size_lock:
            self.cur_size -= decr

    def __getstate__(self):
        state = super(LimitSizeFS,self).__getstate__()
        del state["_size_lock"]
        del state["_file_sizes"]
        return state

    def __setstate__(self, state):
        super(LimitSizeFS,self).__setstate__(state)
        self._size_lock = threading.Lock()

    def getsyspath(self, path, allow_none=False):
        if not allow_none:
            raise NoSysPathError(path)
        return None

    def open(self, path, mode="r"):
        path = relpath(normpath(path))
        with self._size_lock:
            try:
                size = self.getsize(path)
            except ResourceNotFoundError:
                size = 0
            f = super(LimitSizeFS,self).open(path,mode)
            if path not in self._file_sizes:
                self._file_sizes[path] = size
            if "w" in mode:
                self.cur_size -= size
                size = 0
                self._file_sizes[path] = 0
            return LimitSizeFile(self,path,f,mode,size)

    def _ensure_file_size(self, path, size, shrink=False):
        path = relpath(normpath(path))
        with self._size_lock:
            if path not in self._file_sizes:
                self._file_sizes[path] = self.getsize(path)
            cur_size = self._file_sizes[path]
            diff = size - cur_size
            if diff > 0:
                if self.cur_size + diff > self.max_size:
                    raise StorageSpaceError("write")
                self.cur_size += diff
                self._file_sizes[path] = size
            elif diff < 0 and shrink:
                self.cur_size += diff
                self._file_sizes[path] = size

    def copy(self, src, dst, **kwds):
        FS.copy(self,src,dst,**kwds)

    def copydir(self, src, dst, **kwds):
        FS.copydir(self,src,dst,**kwds)

    def move(self, src, dst, **kwds):
        FS.move(self,src,dst,**kwds)
        path = relpath(normpath(src))
        with self._size_lock:
            self._file_sizes.pop(path,None)

    def movedir(self, src, dst, **kwds):
        FS.movedir(self,src,dst,**kwds)

    def remove(self, path):
        size = self.getsize(path)
        super(LimitSizeFS,self).remove(path)
        self._decr_size(size)
        path = relpath(normpath(path))
        with self._size_lock:
            self._file_sizes.pop(path,None)

    def removedir(self, path, recursive=False, force=False):
        size = sum(self.getsize(f) for f in self.walkfiles(path))
        super(LimitSizeFS,self).removedir(path,recursive=recursive,force=force)
        self._decr_size(size)

    def rename(self, src, dst):
        try:
            size = self.getsize(dst)
        except ResourceNotFoundError:
            size = 0
        super(LimitSizeFS,self).rename(src,dst)
        self._decr_size(size)
        path = relpath(normpath(src))
        with self._size_lock:
            self._file_sizes.pop(path,None)


class LimitSizeFile(object):
    """Filelike wrapper class for use by LimitSizeFS."""

    def __init__(self, fs, path, file, mode, size):
        self._lock = fs._lock
        self.fs = fs
        self.path = path
        self.file = file
        self.mode = mode
        self.size = size
        self.closed = False

    @synchronize
    def write(self, data):
        pos = self.file.tell()
        self.size = self.fs._ensure_file_size(self.path,pos+len(data))
        self.file.write(data)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    @synchronize
    def truncate(self, size=None):
        pos = self.file.tell()
        if size is None:
            size = pos
        self.fs._ensure_file_size(self.path,size,shrink=True)
        self.file.truncate(size)
        self.size = size

    #  This is lifted straight from the stdlib's tempfile.py
    def __getattr__(self, name):
        file = self.__dict__['file']
        a = getattr(file, name)
        if not issubclass(type(a), type(0)):
            setattr(self, name, a)
        return a

    def __enter__(self):
        self.file.__enter__()
        return self

    def __exit__(self, exc, value, tb):
        self.close()
        return False

    def __iter__(self):
        return iter(self.file)

