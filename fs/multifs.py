#!/usr/in/env python

from fs import FS, FSError

class MultiFS(FS):

    def __init__(self):
        FS.__init__(self)

        self.fs_sequence = []
        self.fs_lookup =  {}

    def __str__(self):

        return "<MultiFS: %s>" % ", ".join(str(fs) for fs in self.fs_sequence)


    def addfs(self, name, fs):

        if name in self.fs_lookup:
            raise ValueError("Name already exists.")

        self.fs_sequence.append(fs)
        self.fs_lookup[name] = fs


    def removefs(self, name):

        fs = self.fs_lookup[name]
        self.fs_sequence.remove(fs)
        del self.fs_lookup[name]


    def __getitem__(self, name):

        return self.fs_lookup[name]

    def __iter__(self):

        return iter(self.fs_sequence)

    def _delegate_search(self, path):

        for fs in self:
            if fs.exists(path):
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
            return fs.isdir(path)
        return False

    def isfile(self, path):

        fs = self._delegate_search(path)
        if fs is not None:
            return fs.isfile(path)
        return False

    def ishidden(self, path):

        fs = self._delegate_search(path)
        if fs is not None:
            return fs.isfile(path)
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
            if fs.exists(path):
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
            if fs.exists(path):
                return fs.getinfo(path)

        raise FSError("NO_FILE", path)


if __name__ == "__main__":
    
    import fs
    osfs = fs.OSFS('~/')
    import memoryfs

    mem_fs = memoryfs.MemoryFS()
    mem_fs.mkdir('projects/test2', recursive=True)
    mem_fs.mkdir('projects/A', recursive=True)
    mem_fs.mkdir('projects/A/B', recursive=True)


    mem_fs.open("projects/readme.txt", 'w').write("Hello, World!")
    mem_fs.open("projects/readme.txt", 'wa').write("\nSecond Line")
    
    multifs = MultiFS()
    multifs.addfs("osfs", osfs)
    multifs.addfs("mem_fs", mem_fs)
    
    import browsewin
    
    browsewin.browse(multifs)