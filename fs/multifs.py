#!/usr/in/env python

from fs import FS, FSError

class MultiFS(FS):

    def __init__(self):
        FS.__init__(self)

        self.fs_sequence = []
        self.fs_lookup =  {}

    def __str__(self):

        return "<MultiFS: %s>" % ", ".join(str(fs) for fs in self.fs_sequence)


    def add_fs(self, name, fs):

        if name in self.fs_lookup:
            raise ValueError("Name already exists.")

        self.fs_sequence.append(fs)
        self.fs_lookup[name] = fs


    def remove_fs(self, name):

        fs = self.fs_lookup[name]
        self.fs_sequence.remove(fs)
        del self.fs_lookup[name]


    def __getitem__(self, name):

        return self.fs_lookup[name]

    def __iter__(self):

        return iter(self.fs_sequence)

    def _delegate_search(self, path):

        for fs in self:
            if self.exists(path):
                return fs
        return None

    def getsyspath(self, path):

        fs = self._delegate_search(path)
        if fs is not None:
            return fs.getsyspath(path)

        raise FSError("NO_FILE", path)

    def open(self, path, mode="r", buffering=-1, **kwargs):

        for fs in self:
            if fs.exists(path):
                fs_file = fs.open(path, mode, buffering, **kwargs)
                return fs_file

        raise FSError("NO_FILE", path)

    def exists(self, path):

        return self._delegate_search(path) is not None

    def isdir(self, path):

        fs = self._delegate_search(path)
        if fs is not None:
            return fs.isdir()
        return False

    def isfile(self, path):

        fs = self._delegate_search(path)
        if fs is not None:
            return fs.isfile()
        return False

    def ishidden(self, path):

        fs = self._delegate_search(path)
        if fs is not None:
            return fs.isfile()
        return False

    def listdir(self, path="./", *args, **kwargs):

        paths = []
        for fs in self:
            try:
                paths += fs.listdir(path, *args, **kwargs)
            except FSError, e:
                pass

        return list(set(paths))

    def remove(self, path):

        for fs in self:
            if fs.exist(path):
                fs.remove(path)
                return
        raise FSError("NO_FILE", path)

    def removedir(self, path, recursive=False):

        for fs in self:
            if fs.isdir(path):
                fs.removedir(path, recursive)
                return
        raise FSError("NO_DIR", path)


    def getinfo(self, path):

        for fs in self:
            if fs.exist(path):
                return fs.getinfo(path)

        raise FSError("NO_FILE", path)
