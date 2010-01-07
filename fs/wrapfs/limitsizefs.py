"""
fs.wrapfs.limitsizefs
=====================

An FS wrapper class for limiting the size of the underlying FS.

This module provides the class LimitSizeFS, an FS wrapper that can limit the
total size of files stored in the wrapped FS.

"""

from fs.wrapfs import WrapFS


class LimitSizeFS(WrapFS):
    """FS wrapper class to limit total size of files stored."""

    def __init__(self,fs,max_size):
        self.max_size = max_size
        super(LimitSize,self).__init__(fs)
        self.cur_size = sum(self.getsize(f) for f in self.walkfiles())

    @synchronize
    def open(self,path,mode="r"):
        try:
            size = self.getsize(path)
        except ResourceNotFoundError:
            size = 0
        f = super(LimitSize,self).open(path,mode)
        if "w" in mode:
            self.cur_size -= size
            size = 0
        return LimitSizeFile(self,f,mode,size)

    def _acquire_space(self,size):
        new_size = self.cur_size + size
        if new_size > self.max_size:
            raise StorageSpaceError("write")
        self.cur_size = new_size

    #  Disable use of system-level paths, so that copy/copydir have to
    #  fall back to manual writes and pass through _acquire_space.
    getsyspath = FS.getsyspath
    copy = FS.copy
    copydir = FS.copydir

    @synchronize
    def remove(self,path):
        size = self.getsize(path)
        super(LimitSize,self).remove(path)
        self.cur_size -= size

    @synchronize
    def removedir(self,path,recursive=False,force=False):
        size = sum(self.getsize(f) for f in self.walkfiles(path))
        super(LimitSize,self).removedir(path,recursive=recursive,force=force)
        self.cur_size -= size


class LimitSizeFile(object):
    """Filelike wrapper class for use by LimitSizeFS."""

    def __init__(self,fs,file,mode,size):
        self._lock = fs._lock
        self.fs = fs
        self.file = file
        self.mode = mode
        self.size = size
        self.closed = False

    @synchronize
    def write(self,data):
        pos = self.file.tell()
        if pos > self.size:
            self.size = pos
        diff = pos + len(data) - self.size
        if diff <= 0:
            self.file.write(data)
        else:
            self.fs._acquire_space(diff)
            self.file.write(data)
            self.size += diff

    def writelines(self,lines):
        for line in lines:
            self.write(line)

    @synchronize
    def truncate(self,size=None):
        pos = self.file.tell()
        if size is None:
            size = pos
        self.fs._acquire_space(size - self.size)
        self.file.truncate(size)
        self.size = size

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

