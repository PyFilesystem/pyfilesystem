"""
fs.wrapfs.limitsizefs
=====================

An FS wrapper class for limiting the size of the underlying FS.

This module provides the class LimitSizeFS, an FS wrapper that can limit the
total size of files stored in the wrapped FS.

"""

from __future__ import with_statement

from fs.errors import *
from fs.path import *
from fs.base import FS, threading, synchronize
from fs.wrapfs import WrapFS
from fs.filelike import FileWrapper


class LimitSizeFS(WrapFS):
    """FS wrapper class to limit total size of files stored."""

    def __init__(self, fs, max_size):
        super(LimitSizeFS,self).__init__(fs)
        if max_size < 0:
            try:
                max_size = fs.getmeta("total_space") + max_size
            except NoMetaError:
                msg = "FS doesn't report total_size; "\
                      "can't use negative max_size"
                raise ValueError(msg)
        self.max_size = max_size
        self._size_lock = threading.Lock()
        self._file_sizes = PathMap()
        self.cur_size = self._get_cur_size()

    def __getstate__(self):
        state = super(LimitSizeFS,self).__getstate__()
        del state["cur_size"]
        del state["_size_lock"]
        del state["_file_sizes"]
        return state

    def __setstate__(self, state):
        super(LimitSizeFS,self).__setstate__(state)
        self._size_lock = threading.Lock()
        self._file_sizes = PathMap()
        self.cur_size = self._get_cur_size()

    def _get_cur_size(self,path="/"):
        return sum(self.getsize(f) for f in self.walkfiles(path))

    def getsyspath(self, path, allow_none=False):
        #  If people could grab syspaths, they could route around our
        #  size protection; no dice!
        if not allow_none:
            raise NoSysPathError(path)
        return None

    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        path = relpath(normpath(path))
        with self._size_lock:
            try:
                size = self.getsize(path)
            except ResourceNotFoundError:
                size = 0
            f = super(LimitSizeFS,self).open(path,
                                             mode=mode,
                                             buffering=buffering,
                                             errors=errors,
                                             newline=newline,
                                             line_buffering=line_buffering,
                                             **kwargs)
            if "w" not in mode:
                self._set_file_size(path,None,1)
            else:
                self.cur_size -= size
                size = 0
                self._set_file_size(path,0,1)
            return LimitSizeFile(f,mode,size,self,path)

    def _set_file_size(self,path,size,incrcount=None):
        try:
            (cursize,count) = self._file_sizes[path]
        except KeyError:
            count = 0
            try:
                cursize = self.getsize(path)
            except ResourceNotFoundError:
                cursize = 0
        if size is None:
            size = cursize
        if count is not None:
            count += 1
        if count == 0:
            del self._file_sizes[path]
        else:
            self._file_sizes[path] = (size,count)

    def setcontents(self, path, data, chunk_size=64*1024):
        f = None
        try:
            f = self.open(path, 'wb')
            if hasattr(data, 'read'):
                chunk = data.read(chunk_size)
                while chunk:
                    f.write(chunk)
                    chunk = data.read(chunk_size)
            else:
                f.write(data)
        finally:
            if f is not None:
                f.close()

    def _file_closed(self, path):
        self._set_file_size(path,None,-1)

    def _ensure_file_size(self, path, size, shrink=False):
        with self._size_lock:
            try:
                (cur_size,_) = self._file_sizes[path]
            except KeyError:
                try:
                    cur_size = self.getsize(path)
                except ResourceNotFoundError:
                    cur_size = 0
                self._set_file_size(path,cur_size,1)
            diff = size - cur_size
            if diff > 0:
                if self.cur_size + diff > self.max_size:
                    raise StorageSpaceError("write")
                self.cur_size += diff
                self._set_file_size(path,size)
                return size
            elif diff < 0 and shrink:
                self.cur_size += diff
                self._set_file_size(path,size)
                return size
            else:
                return cur_size

    #  We force use of several base FS methods,
    #  since they will fall back to writing out each file
    #  and thus will route through our size checking logic.
    def copy(self, src, dst, **kwds):
        FS.copy(self,src,dst,**kwds)

    def copydir(self, src, dst, **kwds):
        FS.copydir(self,src,dst,**kwds)

    def move(self, src, dst, **kwds):
        if self.getmeta("atomic.rename",False):
            if kwds.get("overwrite",False) or not self.exists(dst):
                try:
                    self.rename(src,dst)
                    return
                except FSError:
                    pass
        FS.move(self, src, dst, **kwds)

    def movedir(self, src, dst, **kwds):
        overwrite = kwds.get("overwrite",False)
        if self.getmeta("atomic.rename",False):
            if kwds.get("overwrite",False) or not self.exists(dst):
                try:
                    self.rename(src,dst)
                    return
                except FSError:
                    pass
        FS.movedir(self,src,dst,**kwds)

    def rename(self, src, dst):
        if self.getmeta("atomic.rename",False):
            try:
                dst_size = self._get_cur_size(dst)
            except ResourceNotFoundError:
                dst_size = 0
            super(LimitSizeFS,self).rename(src,dst)
            with self._size_lock:
                self.cur_size -= dst_size
                self._file_sizes.pop(src,None)
        else:
            if self.isdir(src):
                self.movedir(src,dst)
            else:
                self.move(src,dst)

    def remove(self, path):
        with self._size_lock:
            try:
                (size,_) = self._file_sizes[path]
            except KeyError:
                size = self.getsize(path)
            super(LimitSizeFS,self).remove(path)
            self.cur_size -= size
            self._file_sizes.pop(path,None)

    def removedir(self, path, recursive=False, force=False):
        #  Walk and remove directories by hand, so they we
        #  keep the size accounting precisely up to date.
        for nm in self.listdir(path):
            if not force:
                raise DirectoryNotEmptyError(path)
            cpath = pathjoin(path,nm)
            try:
                if self.isdir(cpath):
                    self.removedir(cpath,force=True)
                else:
                    self.remove(cpath)
            except ResourceNotFoundError:
                pass
        super(LimitSizeFS,self).removedir(path,recursive=recursive)

    def getinfo(self, path):
        info = super(LimitSizeFS,self).getinfo(path)
        try:
            info["size"] = max(self._file_sizes[path][0],info["size"])
        except KeyError:
            pass
        return info

    def getsize(self, path):
        size = super(LimitSizeFS,self).getsize(path)
        try:
            size = max(self._file_sizes[path][0],size)
        except KeyError:
            pass
        return size



class LimitSizeFile(FileWrapper):
    """Filelike wrapper class for use by LimitSizeFS."""

    def __init__(self, file, mode, size, fs, path):
        super(LimitSizeFile,self).__init__(file,mode)
        self.size = size
        self.fs = fs
        self.path = path
        self._lock = fs._lock

    @synchronize
    def _write(self, data, flushing=False):
        pos = self.wrapped_file.tell()
        new_size = self.fs._ensure_file_size(self.path, pos+len(data))
        res = super(LimitSizeFile,self)._write(data, flushing)
        self.size = new_size
        return res

    @synchronize
    def _truncate(self, size):
        new_size = self.fs._ensure_file_size(self.path,size,shrink=True)
        res = super(LimitSizeFile,self)._truncate(size)
        self.size = new_size
        return res

    @synchronize
    def close(self):
        super(LimitSizeFile,self).close()
        self.fs._file_closed(self.path)


