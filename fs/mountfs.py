#!/usr/bin/env python

from fs.base import *
from fs.objecttree import ObjectTree


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


class MountFS(FS):
    """A filesystem that delegates to other filesystems."""

    DirMount = DirMount
    FileMount = FileMount

    def __init__(self, thread_synchronize=True):
        FS.__init__(self, thread_synchronize=thread_synchronize)
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

    def getsyspath(self, path, allow_none=False):
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is self:
            if allow_none:
                return None
            else:
                raise NoSysPathError(path=path)
        return fs.getsyspath(delegate_path, allow_none=allow_none)

    @synchronize
    def desc(self, path):
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is self:
            if fs.isdir(path):
                return "Mount dir"
            else:
                return "Mounted file"
        return "Mounted dir, maps to path %s on %s" % (delegate_path, str(fs))

    @synchronize
    def isdir(self, path):
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            return False
        if fs is self:
            object = self.mount_tree.get(path, None)
            return isinstance(object, dict)
        else:
            return fs.isdir(delegate_path)

    @synchronize
    def isfile(self, path):
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            return False
        if fs is self:
            object = self.mount_tree.get(path, None)
            return type(object) is MountFS.FileMount
        else:
            return fs.isfile(delegate_path)

    @synchronize
    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError(path)

        if fs is self:
            if files_only:
                return []

            paths = self.mount_tree[path].keys()
            return self._listdir_helper(path,
                                        paths,
                                        wildcard,
                                        full,
                                        absolute,
                                        dirs_only,
                                        files_only)
        else:
            paths = fs.listdir(delegate_path,
                               wildcard=wildcard,
                               full=False,
                               absolute=False,
                               dirs_only=dirs_only,
                               files_only=files_only)
            if full or absolute:
                if full:
                    path = abspath(normpath(path))
                else:
                    path = relpath(normpath(path))
                paths = [pathjoin(path, p) for p in paths]

            return paths

    @synchronize
    def makedir(self, path, recursive=False, allow_recreate=False):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is self:
            raise UnsupportedError("make directory", msg="Can only makedir for mounted paths" )
        if not delegate_path:
            return True
        return fs.makedir(delegate_path, recursive=recursive, allow_recreate=allow_recreate)

    @synchronize
    def open(self, path, mode="r", **kwargs):
        path = normpath(path)
        object = self.mount_tree.get(path, None)
        if type(object) is MountFS.FileMount:
            callable = object.open_callable
            return callable(path, mode, **kwargs)

        fs, mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError(path)

        return fs.open(delegate_path, mode, **kwargs)

    @synchronize
    def setcontents(self, path, contents):
        path = normpath(path)
        object = self.mount_tree.get(path, None)
        if type(object) is MountFS.FileMount:
            return super(MountFS,self).setcontents(path,contents)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ParentDirectoryMissingError(path)
        return fs.setcontents(delegate_path,contents)

    @synchronize
    def exists(self, path):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            return False
        if fs is self:
            return path in self.mount_tree
        return fs.exists(delegate_path)

    @synchronize
    def remove(self, path):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            raise UnsupportedError("remove file", msg="Can only remove paths within a mounted dir")
        return fs.remove(delegate_path)

    @synchronize
    def removedir(self, path, recursive=False, force=False):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)

        if fs is None or fs is self:
            raise ResourceInvalidError(path, msg="Can not removedir for an un-mounted path")

        if not force and not fs.isdirempty(delegate_path):
            raise DirectoryNotEmptyError("Directory is not empty: %(path)s")

        return fs.removedir(delegate_path, recursive, force)

    @synchronize
    def rename(self, src, dst):
        fs1, mount_path1, delegate_path1 = self._delegate(src)
        fs2, mount_path2, delegate_path2 = self._delegate(dst)

        if fs1 is not fs2:
            raise OperationFailedError("rename resource", path=src)

        if fs1 is not self:
            return fs1.rename(delegate_path1, delegate_path2)

        path_src = normpath(src)
        path_dst = normpath(dst)

        object = self.mount_tree.get(path_src, None)
        object2 = self.mount_tree.get(path_dst, None)

        if object1 is None:
            raise ResourceNotFoundError(src)

        # TODO!
        raise UnsupportedError("rename resource", path=src)

    @synchronize
    def mountdir(self, path, fs):
        """Mounts a directory on a given path.

        path -- A path within the MountFS
        fs -- A filesystem object to mount

        """
        path = normpath(path)
        self.mount_tree[path] = MountFS.DirMount(path, fs)
    mount = mountdir

    @synchronize
    def mountfile(self, path, open_callable=None, info_callable=None):
        path = normpath(path)
        self.mount_tree[path] = MountFS.FileMount(path, callable, info_callable)

    @synchronize
    def unmount(self,path):
        path = normpath(path)
        del self.mount_tree[path]

    @synchronize
    def getinfo(self, path):
        path = normpath(path)

        fs, mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError(path)

        if fs is self:
            if self.isfile(path):
                return self.mount_tree[path].info_callable(path)
            return {}
        return fs.getinfo(delegate_path)

    @synchronize
    def getsize(self, path):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError(path)

        if fs is self:
            object = self.mount_tree.get(path, None)

            if object is None or isinstance(object, dict):
                raise ResourceNotFoundError(path)

            size = self.mount_tree[path].info_callable(path).get("size", None)
            return size

        return fs.getinfo(delegate_path).get("size", None)

    @synchronize
    def getxattr(self,path,name,default=None):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            return default
        return fs.getxattr(delegate_path,name,default)

    @synchronize
    def setxattr(self,path,name,value):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            raise UnsupportedError("setxattr")
        return fs.setxattr(delegate_path,name,value)

    @synchronize
    def delxattr(self,path,name):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            return True
        return fs.delxattr(delegate_path,name)

    @synchronize
    def listxattrs(self,path):
        path = normpath(path)
        fs, mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            return []
        return fs.listxattrs(delegate_path)
