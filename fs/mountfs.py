"""
fs.mountfs
==========

Contains MountFS class which is a virtual filesystem which can have other filesystems linked as branched directories.

For example, lets say we have two filesystems containing config files and resources respectively::

   [config_fs]
   |-- config.cfg
   `-- defaults.cfg

   [resources_fs]
   |-- images
   |   |-- logo.jpg
   |   `-- photo.jpg
   `-- data.dat

We can combine these filesystems in to a single filesystem with the following code::

    from fs.mountfs import MountFS
    combined_fs = MountFS()
    combined_fs.mountdir('config', config_fs)
    combined_fs.mountdir('resources', resources_fs)

This will create a single filesystem where paths under `config` map to `config_fs`, and paths under `resources` map to `resources_fs`::

    [combined_fs]
    |-- config
    |   |-- config.cfg
    |   `-- defaults.cfg
    `-- resources
        |-- images
        |   |-- logo.jpg
        |   `-- photo.jpg
        `-- data.dat

Now both filesystems can be accessed with the same path structure::

    print combined_fs.getcontents('/config/defaults.cfg')
    read_jpg(combined_fs.open('/resources/images/logo.jpg')

"""

from fs.base import *
from fs.errors import *
from fs.path import *
from fs import _thread_synchronize_default
from fs import iotools


class DirMount(object):
    def __init__(self, path, fs):
        self.path = path
        self.fs = fs

    def __str__(self):
        return "<DirMount %s, %s>" % (self.path, self.fs)

    def __repr__(self):
        return "<DirMount %s, %s>" % (self.path, self.fs)

    def __unicode__(self):
        return u"<DirMount %s, %s>" % (self.path, self.fs)


class FileMount(object):
    def __init__(self, path, open_callable, info_callable=None):
        self.open_callable = open_callable
        def no_info_callable(path):
            return {}
        self.info_callable = info_callable or no_info_callable


class MountFS(FS):
    """A filesystem that delegates to other filesystems."""

    _meta = { 'virtual': True,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              }

    DirMount = DirMount
    FileMount = FileMount

    def __init__(self, auto_close=True, thread_synchronize=_thread_synchronize_default):
        self.auto_close = auto_close
        super(MountFS, self).__init__(thread_synchronize=thread_synchronize)
        self.mount_tree = PathMap()

    def __str__(self):
        return "<%s [%s]>" % (self.__class__.__name__,self.mount_tree.items(),)

    __repr__ = __str__

    def __unicode__(self):
        return u"<%s [%s]>" % (self.__class__.__name__,self.mount_tree.items(),)

    def _delegate(self, path):
        path = abspath(normpath(path))
        object = None
        head_path = "/"
        tail_path = path

        for prefix in recursepath(path):
            try:
                object = self.mount_tree[prefix]
            except KeyError:
                pass
            else:
                head_path = prefix
                tail_path = path[len(head_path):]

        if type(object) is MountFS.DirMount:
            return object.fs, head_path, tail_path

        if type(object) is MountFS.FileMount:
            return self, "/", path

        try:
            self.mount_tree.iternames(path).next()
        except StopIteration:
            return None, None, None
        else:
            return self, "/", path

    @synchronize
    def close(self):
        # Explicitly closes children if requested
        if self.auto_close:
            for mount in self.mount_tree.itervalues():
                mount.fs.close()
        # Free references (which may incidently call the close method of the child filesystems)
        self.mount_tree.clear()
        super(MountFS, self).close()

    def getsyspath(self, path, allow_none=False):
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self or fs is None:
            if allow_none:
                return None
            else:
                raise NoSysPathError(path=path)
        return fs.getsyspath(delegate_path, allow_none=allow_none)

    def getpathurl(self, path, allow_none=False):
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self or fs is None:
            if allow_none:
                return None
            else:
                raise NoPathURLError(path=path)
        return fs.getpathurl(delegate_path, allow_none=allow_none)

    @synchronize
    def desc(self, path):
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self:
            if fs.isdir(path):
                return "Mount dir"
            else:
                return "Mounted file"
        return "Mounted dir, maps to path %s on %s" % (abspath(delegate_path) or '/', str(fs))

    @synchronize
    def isdir(self, path):
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is None:
            path = normpath(path)
            if path in ("/", ""):
                return True
            return False
        if fs is self:
            obj = self.mount_tree.get(path, None)
            return not isinstance(obj, MountFS.FileMount)
        return fs.isdir(delegate_path)

    @synchronize
    def isfile(self, path):
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is None:
            return False
        if fs is self:
            obj = self.mount_tree.get(path, None)
            return isinstance(obj, MountFS.FileMount)
        return fs.isfile(delegate_path)

    @synchronize
    def exists(self, path):
        if path in ("/", ""):
            return True
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is None:
            return False
        if fs is self:
            return True
        return fs.exists(delegate_path)

    @synchronize
    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        fs, _mount_path, delegate_path = self._delegate(path)

        if fs is None:
            if path in ("/", ""):
                return []
            raise ResourceNotFoundError("path")

        elif fs is self:
            paths = self.mount_tree.names(path)
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
            for nm in self.mount_tree.names(path):
                if nm not in paths:
                    if dirs_only:
                        if self.isdir(pathjoin(path,nm)):
                            paths.append(nm)
                    elif files_only:
                        if self.isfile(pathjoin(path,nm)):
                            paths.append(nm)
                    else:
                        paths.append(nm)
            if full or absolute:
                if full:
                    path = relpath(normpath(path))
                else:
                    path = abspath(normpath(path))
                paths = [pathjoin(path, p) for p in paths]

            return paths

    @synchronize
    def ilistdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        fs, _mount_path, delegate_path = self._delegate(path)

        if fs is None:
            if path in ("/", ""):
                return
            raise ResourceNotFoundError(path)

        if fs is self:
            paths = self.mount_tree.names(path)
            for path in self._listdir_helper(path,paths,wildcard,full,absolute,dirs_only,files_only):
                yield path
        else:
            paths = fs.ilistdir(delegate_path,
                                wildcard=wildcard,
                                full=False,
                                absolute=False,
                                dirs_only=dirs_only)
            extra_paths = set(self.mount_tree.names(path))
            if full:
                pathhead = relpath(normpath(path))
                def mkpath(p):
                    return pathjoin(pathhead,p)
            elif absolute:
                pathhead = abspath(normpath(path))
                def mkpath(p):
                    return pathjoin(pathhead,p)
            else:
                def mkpath(p):
                    return p
            for p in paths:
                if p not in extra_paths:
                    yield mkpath(p)
            for p in extra_paths:
                if dirs_only:
                    if self.isdir(pathjoin(path,p)):
                        yield mkpath(p)
                elif files_only:
                    if self.isfile(pathjoin(path,p)):
                        yield mkpath(p)
                else:
                    yield mkpath(p)

    @synchronize
    def makedir(self, path, recursive=False, allow_recreate=False):
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self or fs is None:
            raise UnsupportedError("make directory", msg="Can only makedir for mounted paths")
        if not delegate_path:
            if allow_recreate:
                return
            else:
                raise DestinationExistsError(path, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
        return fs.makedir(delegate_path, recursive=recursive, allow_recreate=allow_recreate)

    @synchronize
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        obj = self.mount_tree.get(path, None)
        if type(obj) is MountFS.FileMount:
            callable = obj.open_callable
            return callable(path, mode, **kwargs)

        fs, _mount_path, delegate_path = self._delegate(path)

        if fs is self or fs is None:
            raise ResourceNotFoundError(path)

        return fs.open(delegate_path, mode, **kwargs)

    @synchronize
    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=64*1024):
        obj = self.mount_tree.get(path, None)
        if type(obj) is MountFS.FileMount:
            return super(MountFS, self).setcontents(path,
                                                    data,
                                                    encoding=encoding,
                                                    errors=errors,
                                                    chunk_size=chunk_size)
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self or fs is None:
            raise ParentDirectoryMissingError(path)
        return fs.setcontents(delegate_path, data, encoding=encoding, errors=errors, chunk_size=chunk_size)

    @synchronize
    def createfile(self, path, wipe=False):
        obj = self.mount_tree.get(path, None)
        if type(obj) is MountFS.FileMount:
            return super(MountFS, self).createfile(path, wipe=wipe)
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self or fs is None:
            raise ParentDirectoryMissingError(path)
        return fs.createfile(delegate_path, wipe=wipe)

    @synchronize
    def remove(self, path):
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self or fs is None:
            raise UnsupportedError("remove file", msg="Can only remove paths within a mounted dir")
        return fs.remove(delegate_path)

    @synchronize
    def removedir(self, path, recursive=False, force=False):
        path = normpath(path)
        if path in ('', '/'):
            raise RemoveRootError(path)
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is self or fs is None:
            raise ResourceInvalidError(path, msg="Can not removedir for an un-mounted path")
        return fs.removedir(delegate_path, recursive, force)

    @synchronize
    def rename(self, src, dst):
        fs1, _mount_path1, delegate_path1 = self._delegate(src)
        fs2, _mount_path2, delegate_path2 = self._delegate(dst)

        if fs1 is not fs2:
            raise OperationFailedError("rename resource", path=src)

        if fs1 is not self:
            return fs1.rename(delegate_path1, delegate_path2)

        object = self.mount_tree.get(src, None)
        _object2 = self.mount_tree.get(dst, None)

        if object is None:
            raise ResourceNotFoundError(src)

        raise UnsupportedError("rename resource", path=src)

    @synchronize
    def move(self,src,dst,**kwds):
        fs1, _mount_path1, delegate_path1 = self._delegate(src)
        fs2, _mount_path2, delegate_path2 = self._delegate(dst)
        if fs1 is fs2 and fs1 is not self:
            fs1.move(delegate_path1,delegate_path2,**kwds)
        else:
            super(MountFS,self).move(src,dst,**kwds)

    @synchronize
    def movedir(self,src,dst,**kwds):
        fs1, _mount_path1, delegate_path1 = self._delegate(src)
        fs2, _mount_path2, delegate_path2 = self._delegate(dst)
        if fs1 is fs2 and fs1 is not self:
            fs1.movedir(delegate_path1,delegate_path2,**kwds)
        else:
            super(MountFS,self).movedir(src,dst,**kwds)

    @synchronize
    def copy(self,src,dst,**kwds):
        fs1, _mount_path1, delegate_path1 = self._delegate(src)
        fs2, _mount_path2, delegate_path2 = self._delegate(dst)
        if fs1 is fs2 and fs1 is not self:
            fs1.copy(delegate_path1,delegate_path2,**kwds)
        else:
            super(MountFS,self).copy(src,dst,**kwds)

    @synchronize
    def copydir(self,src,dst,**kwds):
        fs1, _mount_path1, delegate_path1 = self._delegate(src)
        fs2, _mount_path2, delegate_path2 = self._delegate(dst)
        if fs1 is fs2 and fs1 is not self:
            fs1.copydir(delegate_path1,delegate_path2,**kwds)
        else:
            super(MountFS,self).copydir(src,dst,**kwds)

    @synchronize
    def mountdir(self, path, fs):
        """Mounts a host FS object on a given path.

        :param path: A path within the MountFS
        :param fs: A filesystem object to mount

        """
        path = abspath(normpath(path))
        self.mount_tree[path] = MountFS.DirMount(path, fs)
    mount = mountdir

    @synchronize
    def mountfile(self, path, open_callable=None, info_callable=None):
        """Mounts a single file path.

        :param path: A path within the MountFS
        :param open_callable: A callable that returns a file-like object,
            `open_callable` should have the same signature as :py:meth:`~fs.base.FS.open`
        :param info_callable: A callable that returns a dictionary with information regarding the file-like object,
            `info_callable` should have the same signagture as :py:meth:`~fs.base.FS.getinfo`

        """
        self.mount_tree[path] = MountFS.FileMount(path, open_callable, info_callable)

    @synchronize
    def unmount(self, path):
        """Unmounts a path.

        :param path: Path to unmount
        :return: True if a path was unmounted, False if the path was already unmounted
        :rtype: bool

        """
        try:
            del self.mount_tree[path]
        except KeyError:
            return False
        else:
            return True

    @synchronize
    def settimes(self, path, accessed_time=None, modified_time=None):
        path = normpath(path)
        fs, _mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError(path)

        if fs is self:
            raise UnsupportedError("settimes")
        fs.settimes(delegate_path, accessed_time, modified_time)

    @synchronize
    def getinfo(self, path):
        path = normpath(path)

        fs, _mount_path, delegate_path = self._delegate(path)

        if fs is None:
            if path in ("/", ""):
                return {}
            raise ResourceNotFoundError(path)

        if fs is self:
            if self.isfile(path):
                return self.mount_tree[path].info_callable(path)
            return {}
        return fs.getinfo(delegate_path)

    @synchronize
    def getsize(self, path):
        path = normpath(path)
        fs, _mount_path, delegate_path = self._delegate(path)

        if fs is None:
            raise ResourceNotFoundError(path)

        if fs is self:
            object = self.mount_tree.get(path, None)

            if object is None:
                raise ResourceNotFoundError(path)
            if not isinstance(object,MountFS.FileMount):
                raise ResourceInvalidError(path)

            size = object.info_callable(path).get("size", None)
            return size

        return fs.getinfo(delegate_path).get("size", None)

    @synchronize
    def getxattr(self,path,name,default=None):
        path = normpath(path)
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is None:
            if path in ("/", ""):
                return default
            raise ResourceNotFoundError(path)
        if fs is self:
            return default
        return fs.getxattr(delegate_path,name,default)

    @synchronize
    def setxattr(self,path,name,value):
        path = normpath(path)
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            raise UnsupportedError("setxattr")
        return fs.setxattr(delegate_path,name,value)

    @synchronize
    def delxattr(self,path,name):
        path = normpath(path)
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            return True
        return fs.delxattr(delegate_path, name)

    @synchronize
    def listxattrs(self,path):
        path = normpath(path)
        fs, _mount_path, delegate_path = self._delegate(path)
        if fs is None:
            raise ResourceNotFoundError(path)
        if fs is self:
            return []
        return fs.listxattrs(delegate_path)


