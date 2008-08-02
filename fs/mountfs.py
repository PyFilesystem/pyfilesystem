#!/usr/bin/env python

from fs import FS, FSError, pathjoin, pathsplit, print_fs, _iteratepath, normpath, makeabsolute, makerelative
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


    def __init__(self):
        self.mount_tree = ObjectTree()

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
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is self:
            if fs.isdir(path):
                return "Mount dir"
            else:
                return "Mounted file"
        return "Mounted dir, maps to path %s on %s" % (delegate_path, str(fs))

    def isdir(self, path):
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError("NO_RESOURCE", path)

        if fs is self:
            object = self.mount_tree.get(path, None)
            return isinstance(object, dict)
        else:
            return fs.isdir(delegate_path)

    def isfile(self, path):

        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            return ResourceNotFoundError("NO_RESOURCE", path)

        if fs is self:
            object = self.mount_tree.get(path, None)
            return type(object) is MountFS.FileMount
        else:
            return fs.isfile(delegate_path)


    def listdir(self, path="/", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):
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

    def open(self, path, mode="r", **kwargs):

        object = self.mount_tree.get(path, None)
        if type(object) is MountFS.FileMount:
            callable = object.open_callable
            return callable(path, mode, **kwargs)

        fs, mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError("NO_FILE", path)

        return fs.open(delegate_path, mode, **kwargs)


    def mountdir(self, path, fs):
        path = normpath(path)
        self.mount_tree[path] = MountFS.DirMount(path, fs)

    def mountfile(self, path, open_callable=None, info_callable=None):
        path = normpath(path)
        self.mount_tree[path] = MountFS.FileMount(path, callable, info_callable)

    def getinfo(self, path):

        path = normpath(path)

        fs, mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError("NO_RESOURCE", path)

        if fs is self:
            if self.isfile(path):
                return self.mount_tree[path].info_callable(path)
            return {}
        return fs.getinfo(delegate_path)
#
#class MountFS(FS):
#
#    class Mount(object):
#        def __init__(self, path, memory_fs, value, mode):
#            self.path = path
#            memory_fs._on_close_memory_file(path, self)
#            self.fs = None
#
#        def __str__(self):
#            return "Mount pont: %s, %s" % (self.path, str(self.fs))
#
#    def get_mount(self, path, memory_fs, value, mode):
#
#        dir_entry = memory_fs._get_dir_entry(path)
#        if dir_entry is None or dir_entry.data is None:
#            return MountFS.Mount(path, memory_fs, value, mode)
#        else:
#            return dir_entry.data
#
#    def __init__(self):
#        self.mounts = {}
#        self.mem_fs = MemoryFS(file_factory=self.get_mount)
#
#    def _delegate(self, path):
#        path_components = list(_iteratepath(path))
#
#        current_dir = self.mem_fs.root
#        for i, path_component in enumerate(path_components):
#
#            if current_dir is None:
#                return None, None, None
#
#            if '.mount' in current_dir.contents:
#                break
#
#            dir_entry = current_dir.contents.get(path_component, None)
#            current_dir = dir_entry
#        else:
#            i = len(path_components)
#
#        if '.mount' in current_dir.contents:
#
#                mount_point = '/'.join(path_components[:i])
#                mount_filename = pathjoin(mount_point, '.mount')
#
#                mount = self.mem_fs.open(mount_filename, 'r')
#                delegate_path = '/'.join(path_components[i:])
#                return mount.fs, mount_point, delegate_path
#
#        return self, "", path
#
#    def desc(self, path):
#        fs, mount_path, delegate_path = self._delegate(path)
#        if fs is self:
#            return "Mount dir"
#
#        return "Mounted dir, maps to path %s on %s" % (delegate_path, str(fs))
#
#    def isdir(self, path):
#        fs, mount_path, delegate_path = self._delegate(path)
#        if fs is None:
#            return False
#
#        if fs is self:
#            return True
#        else:
#            return fs.isdir(delegate_path)
#
#    def listdir(self, path="/", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):
#        fs, mount_path, delegate_path = self._delegate(path)
#
#        if fs is None:
#            raise ResourceNotFoundError("NO_DIR", path)
#
#        if fs is self:
#            if files_only:
#                return []
#            return self.mem_fs.listdir(path,
#                                       wildcard=wildcard,
#                                       full=full,
#                                       absolute=absolute,
#                                       hidden=hidden,
#                                       dirs_only=True,
#                                       files_only=False)
#        else:
#            paths = fs.listdir(delegate_path,
#                               wildcard=wildcard,
#                               full=full,
#                               absolute=absolute,
#                               hidden=hidden,
#                               dirs_only=dirs_only,
#                               files_only=files_only)
#            if full or absolute:
#                if full:
#                    mount_path = makeabsolute(mount_path)
#                else:
#                    mount_path = makerelative(mount_path)
#                paths = [pathjoin(mount_path, path) for path in paths]
#
#            return paths
#
#    def mount(self, name, path, fs):
#        self.mem_fs.mkdir(path, recursive=True)
#        mount_filename = pathjoin(path, '.mount')
#        mount = self.mem_fs.open(mount_filename, 'w')
#        mount.name = name
#        mount.fs = fs
#
#        self.mounts[name] = (path, fs)

if __name__ == "__main__":

    fs1 = MemoryFS()
    fs1.mkdir("Memroot/B/C/D", recursive=True)
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