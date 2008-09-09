#!/usr/in/env python

from fs import FS, FSError

class MultiFS(FS):

    """A MultiFS is a filesystem that delegates to a sequence of other filesystems.
    Operations on the MultiFS will try each 'child' filesystem in order, until it
    succeeds. In effect, creating a filesystem that combines the files and dirs of
    its children.

    """

    def __init__(self):
        FS.__init__(self, thread_syncronize=True)

        self.fs_sequence = []
        self.fs_lookup =  {}

    def __str__(self):
        self._lock.acquire()
        try:
            return "<MultiFS: %s>" % ", ".join(str(fs) for fs in self.fs_sequence)
        finally:
            self._lock.release()

    def addfs(self, name, fs):
        """Adds a filesystem to the MultiFS.

        name -- A unique name to refer to the filesystem being added
        fs -- The filesystem to add

        """
        self._lock.acquire()
        try:
            if name in self.fs_lookup:
                raise ValueError("Name already exists.")

            self.fs_sequence.append(fs)
            self.fs_lookup[name] = fs
        finally:
            self._lock.release()

    def removefs(self, name):
        """Removes a filesystem from the sequence.

        name -- The name of the filesystem, as used in addfs

        """
        self._lock.acquire()
        try:
            if name not in self.fs_lookup:
                raise ValueError("No filesystem called '%s'"%name)
            fs = self.fs_lookup[name]
            self.fs_sequence.remove(fs)
            del self.fs_lookup[name]
        finally:
            self._lock.release()

    def __getitem__(self, name):
        self._lock.acquire()
        try:
            return self.fs_lookup[name]
        finally:
            self._lock.release()

    def __iter__(self):
        self._lock.acquire()
        try:
            return iter(self.fs_sequence[:])
        finally:
            self._lock.release()


    def _delegate_search(self, path):
        for fs in self:
            if fs.exists(path):
                return fs
        return None

    def which(self, path):
        """Retrieves the filesystem that a given path would delegate to.
        Returns a tuple of the filesystem's name and the filesystem object itself.

        path -- A path in MultiFS

        """
        self._lock.acquire()
        try:
            for fs in self:
                if fs.exists(path):
                    for fs_name, fs_object in self.fs_lookup.iteritems():
                        if fs is fs_object:
                            return fs_name, fs
            raise ResourceNotFoundError("NO_RESOURCE", path, msg="Path does not map to any filesystem: %(path)s")
        finally:
            self._lock.release()

    def getsyspath(self, path, allow_none=False):
        self._lock.acquire()
        try:
            fs = self._delegate_search(path)
            if fs is not None:
                return fs.getsyspath(path, allow_none=allow_none)
            raise ResourceNotFoundError("NO_RESOURCE", path)
        finally:
            self._lock.release()

    def desc(self, path):
        self._lock.acquire()
        try:
            if not self.exists(path):
                raise ResourceNotFoundError("NO_RESOURCE", path)

            name, fs = self.which(path)
            if name is None:
                return ""
            return "%s, on %s (%s)" % (fs.desc(path), name, fs)
        finally:
            self._lock.release()


    def open(self, path, mode="r",**kwargs):
        self._lock.acquire()
        try:
            for fs in self:
                if fs.exists(path):
                    fs_file = fs.open(path, mode, **kwargs)
                    return fs_file

            raise ResourceNotFoundError("NO_FILE", path)
        finally:
            self._lock.release()

    def exists(self, path):
        self._lock.acquire()
        try:
            return self._delegate_search(path) is not None
        finally:
            self._lock.release()

    def isdir(self, path):
        self._lock.acquire()
        try:
            fs = self._delegate_search(path)
            if fs is not None:
                return fs.isdir(path)
            return False
        finally:
            self._lock.release()

    def isfile(self, path):
        self._lock.acquire()
        try:
            fs = self._delegate_search(path)
            if fs is not None:
                return fs.isfile(path)
            return False
        finally:
            self._lock.release()

    def ishidden(self, path):
        self._lock.acquire()
        try:
            fs = self._delegate_search(path)
            if fs is not None:
                return fs.isfile(path)
            return False
        finally:
            self._lock.release()

    def listdir(self, path="./", *args, **kwargs):
        self._lock.acquire()
        try:
            paths = []
            for fs in self:
                try:
                    paths += fs.listdir(path, *args, **kwargs)
                except FSError, e:
                    pass

            return list(set(paths))
        finally:
            self._lock.release()

    def remove(self, path):
        self._lock.acquire()
        try:
            for fs in self:
                if fs.exists(path):
                    fs.remove(path)
                    return
            raise ResourceNotFoundError("NO_FILE", path)
        finally:
            self._lock.release()

    def removedir(self, path, recursive=False):
        self._lock.acquire()
        try:
            for fs in self:
                if fs.isdir(path):
                    fs.removedir(path, recursive)
                    return
            raise ResourceNotFoundError("NO_DIR", path)
        finally:
            self._lock.release()

    def rename(self, src, dst):
        if not issamedir(src, dst):
            raise ValueError("Destination path must the same directory (user the move method for moving to a different directory)")
        self._lock.acquire()
        try:
            for fs in self:
                if fs.exists(src):
                    fs.rename(src, dst)
                    return
            raise FSError("NO_RESOURCE", path)
        finally:
            self._lock.release()

    def getinfo(self, path):
        self._lock.acquire()
        try:
            for fs in self:
                if fs.exists(path):
                    return fs.getinfo(path)

            raise ResourceNotFoundError("NO_FILE", path)
        finally:
            self._lock.release()


if __name__ == "__main__":

    import fs
    import osfs
    osfs = osfs.OSFS('~/')
    import memoryfs

    mem_fs = memoryfs.MemoryFS()
    mem_fs.makedir('projects/test2', recursive=True)
    mem_fs.makedir('projects/A', recursive=True)
    mem_fs.makedir('projects/A/B', recursive=True)


    mem_fs.open("projects/test2/readme.txt", 'w').write("Hello, World!")
    mem_fs.open("projects/A/readme.txt", 'w').write("\nSecond Line")

    multifs = MultiFS()
    multifs.addfs("osfs", osfs)
    multifs.addfs("mem_fs", mem_fs)

    import browsewin

    browsewin.browse(multifs)