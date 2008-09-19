#!/usr/bin/env python

from fs import *
from objecttree import ObjectTree
from memoryfs import MemoryFS

class MountFS(FS):

    class DirMount(object):
        def __init__(self, path, fs):
            self.path = path
            self.fs = fs

        def __str__(self):
            return "Mount point: %s"%self.path

    class FileMount(object):
        def __init__(self, path, open_callable, info_callable=None):
            self.open_callable = open_callable
            def no_info_callable(path):
                return {}
            self.info_callable = info_callable or no_info_callable

    def __init__(self, thread_syncronize=True):
        FS.__init__(self, thread_syncronize=thread_syncronize)
        self.mount_tree = ObjectTree()

    def __str__(self):
        return "<MountFS>"

    __repr__ = __str__

    def __unicode__(self):
        return unicode(self.__str__())

    def _delegate(self, path):
        path = normpath(path)
        head_path, object, tail_path = self.mount_tree.partialget(path)

        if type(object) is MountFS.DirMount:
            dirmount = object
            return dirmount.fs, head_path, tail_path

        if object is None:
            return None, None, None

        return self, head_path, tail_path

    def desc(self, path):
        self._lock.acquire()
        try:
            fs, mount_path, delegate_path = self._delegate(path)
            if fs is self:
                if fs.isdir(path):
                    return "Mount dir"
                else:
                    return "Mounted file"
            return "Mounted dir, maps to path %s on %s" % (delegate_path, str(fs))
        finally:
            self._lock.release()

    def isdir(self, path):
        self._lock.acquire()
        try:
            fs, mount_path, delegate_path = self._delegate(path)
            if fs is None:
                raise ResourceNotFoundError("NO_RESOURCE", path)

            if fs is self:
                object = self.mount_tree.get(path, None)
                return isinstance(object, dict)
            else:
                return fs.isdir(delegate_path)
        finally:
            self._lock.release()

    def isfile(self, path):

        self._lock.acquire()
        try:
            fs, mount_path, delegate_path = self._delegate(path)
            if fs is None:
                return ResourceNotFoundError("NO_RESOURCE", path)

            if fs is self:
                object = self.mount_tree.get(path, None)
                return type(object) is MountFS.FileMount
            else:
                return fs.isfile(delegate_path)
        finally:
            self._lock.release()

    def listdir(self, path="/", wildcard=None, full=False, absolute=False, hidden=True, dirs_only=False, files_only=False):

        self._lock.acquire()
        try:
            path = normpath(path)
            fs, mount_path, delegate_path = self._delegate(path)

            if fs is None:
                raise ResourceNotFoundError("NO_DIR", path)

            if fs is self:
                if files_only:
                    return []

                paths = self.mount_tree[path].keys()
                return self._listdir_helper(path,
                                            paths,
                                            wildcard,
                                            full,
                                            absolute,
                                            hidden,
                                            dirs_only,
                                            files_only)
            else:
                paths = fs.listdir(delegate_path,
                                   wildcard=wildcard,
                                   full=False,
                                   absolute=False,
                                   hidden=hidden,
                                   dirs_only=dirs_only,
                                   files_only=files_only)
                if full or absolute:
                    if full:
                        path = makeabsolute(path)
                    else:
                        path = makerelative(path)
                    paths = [pathjoin(path, p) for p in paths]

                return paths
        finally:
            self._lock.release()

    def makedir(self, path, mode=0777, recursive=False, allow_recreate=False):
        path = normpath(path)
        self._lock.acquire()
        try:
            fs, mount_path, delegate_path = self._delegate(path)
            if fs is self:
                raise UnsupportedError("UNSUPPORTED", msg="Can only makedir for mounted paths" )
            return fs.makedir(delegate_path, mode, recursive=recursive, allow_recreate=allow_recreate)
        finally:
            self._lock.release()

    def open(self, path, mode="r", **kwargs):

        self._lock.acquire()
        try:
            path = normpath(path)
            object = self.mount_tree.get(path, None)
            if type(object) is MountFS.FileMount:
                callable = object.open_callable
                return callable(path, mode, **kwargs)

            fs, mount_path, delegate_path = self._delegate(path)

            if fs is None:
                raise ResourceNotFoundError("NO_FILE", path)

            return fs.open(delegate_path, mode, **kwargs)

        finally:
            self._lock.release()

    def exists(self, path):

        self._lock.acquire()
        try:

            path = normpath(path)
            fs, mount_path, delegate_path = self._delegate(path)

            if fs is None:
                return False

            if fs is self:
                return path in self.mount_tree

            return fs.exists(delegate_path)

        finally:
            self._lock.release()

    def remove(self, path):
        self._lock.acquire()
        try:
            path = normpath(path)
            fs, mount_path, delegate_path = self._delegate(path)
            if fs is None:
                raise ResourceNotFoundError("NO_FILE", path)
            if fs is self:
                raise UnsupportedError("UNSUPPORTED", msg="Can only remove paths within a mounted dir" )
            return fs.remove(delegate_path)

        finally:
            self._lock.release()

    def removedir(self, path, recursive=False):

        self._lock.acquire()
        try:

            path = normpath(path)
            fs, mount_path, delegate_path = self._delegate(path)

            if fs is None or fs is self:
                raise OperationFailedError("REMOVEDIR_FAILED", path, msg="Can not removedir for an un-mounted path")

            if not fs.isdirempty(delegate_path):
                raise OperationFailedError("REMOVEDIR_FAILED", "Directory is not empty: %(path)s")

            return fs.removedir(delegate_path, recursive)

        finally:
            self._lock.release()

    def rename(self, src, dst):

        if not issamedir(src, dst):
            raise ValueError("Destination path must the same directory (user the move method for moving to a different directory)")

        self._lock.acquire()
        try:
            fs1, mount_path1, delegate_path1 = self._delegate(src)
            fs2, mount_path2, delegate_path2 = self._delegate(dst)

            if fs1 is not fs2:
                raise OperationFailedError("RENAME_FAILED", src)

            if fs1 is not self:
                return fs1.rename(delegate_path1, delegate_path2)

            path_src = normpath(src)
            path_dst = normpath(dst)

            object = self.mount_tree.get(path_src, None)
            object2 = self.mount_tree.get(path_dst, None)

            if object1 is None:
                raise NoResourceError("NO_RESOURCE", src)

            # TODO!

            raise UnsupportedError("UNSUPPORTED", src)
        finally:
            self._lock.release()

    def mountdir(self, path, fs):
        """Mounts a directory on a given path.

        path -- A path within the MountFS
        fs -- A filesystem object to mount

        """
        self._lock.acquire()
        try:
            path = normpath(path)
            self.mount_tree[path] = MountFS.DirMount(path, fs)
        finally:
            self._lock.release()

    def mountfile(self, path, open_callable=None, info_callable=None):
        self._lock.acquire()
        try:
            path = normpath(path)
            self.mount_tree[path] = MountFS.FileMount(path, callable, info_callable)
        finally:
            self._lock.release()

    def getinfo(self, path):

        self._lock.acquire()
        try:
            path = normpath(path)

            fs, mount_path, delegate_path = self._delegate(path)

            if fs is None:
                raise ResourceNotFoundError("NO_RESOURCE", path)

            if fs is self:
                if self.isfile(path):
                    return self.mount_tree[path].info_callable(path)
                return {}
            return fs.getinfo(delegate_path)
        finally:
            self._lock.release()

    def getsize(self, path):
        self._lock.acquire()
        try:
            path = normpath(path)
            fs, mount_path, delegate_path = self._delegate(path)

            if fs is None:
                raise ResourceNotFoundError("NO_FILE", path)

            if fs is self:
                object = self.mount_tree.get(path, None)

                if object is None or isinstance(object, dict):
                    raise ResourceNotFoundError("NO_FILE", path)

                size = self.mount_tree[path].info_callable(path).get("size", None)
                return size

            return fs.getinfo(delegate_path).get("size", None)
        except:
            self._lock.release()


if __name__ == "__main__":

    help(MountFS)

    fs1 = MemoryFS()
    fs1.makedir("Memroot/B/C/D", recursive=True)
    fs1.open("test.txt", 'w').write("Hello, World!")

    #print_fs(fs1)

    mountfs = MountFS()

    mountfs.mountdir('1/2', fs1)
    mountfs.mountdir('1/another', fs1)

    def testfile(*args, **kwargs):
        print args, kwargs

    def testfile_info(*args, **kwargs):
        print "testfile_info", args, kwargs
        return {'size':100}

    mountfs.mountfile('filedir/file.txt', testfile, testfile_info)

    print mountfs.getinfo("filedir/file.txt")

    #print mountfs.listdir('1/2/Memroot/B/C')

    print mountfs.isdir("1")

    print mountfs.desc('1/2/Memroot/B')
    print_fs(mountfs)

    import browsewin
    browsewin.browse(mountfs)

    print mountfs.getinfo("1/2")

    #print mountfs._delegate('1/2/Memroot/B')