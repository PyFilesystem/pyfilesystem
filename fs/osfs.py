#!/usr/bin/env python

import os

from base import *
from helpers import *

try:
    import xattr
except ImportError:
    xattr = None

class OSFS(FS):
    """Expose the underlying operating-system filesystem as an FS object.

    This is the most basic of filesystems, which simply shadows the underlaying
    filesytem of the OS.  Most of its methods simply defer to the corresponding
    methods in the os and os.path modules.

    """

    def __init__(self, root_path, dir_mode=0700, thread_synchronize=True):
        FS.__init__(self, thread_synchronize=thread_synchronize)

        expanded_path = normpath(os.path.abspath(os.path.expanduser(os.path.expandvars(root_path))))
        if not os.path.exists(expanded_path):
            raise DirectoryNotFoundError(expanded_path, msg="Root directory does not exist: %(path)s")
        if not os.path.isdir(expanded_path):
            raise InvalidResourceError(expanded_path, msg="Root path is not a directory: %(path)s")

        self.root_path = normpath(os.path.abspath(expanded_path))
        self.dir_mode = dir_mode

    def __str__(self):
        return "<OSFS: %s>" % self.root_path

    def getsyspath(self, path, allow_none=False):
        sys_path = os.path.join(self.root_path, makerelative(normpath(path))).replace('/', os.sep)
        return sys_path

    def open(self, path, mode="r", **kwargs):
        mode = filter(lambda c: c in "rwabt+",mode)
        try:
            f = open(self.getsyspath(path), mode, kwargs.get("buffering", -1))
        except IOError, e:
            if e.errno == 2:
                raise FileNotFoundError(path)
            raise OperationFailedError("open file", details=e, msg=str(e))

        return f

    def exists(self, path):
        path = self.getsyspath(path)
        return os.path.exists(path)

    def isdir(self, path):
        path = self.getsyspath(path)
        return os.path.isdir(path)

    def isfile(self, path):
        path = self.getsyspath(path)
        return os.path.isfile(path)

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        try:
            paths = os.listdir(self.getsyspath(path))
        except (OSError, IOError), e:
            if e.errno == 2:
                raise ResourceNotFoundError(path)
            if e.errno in (20,22,):
                raise ResourceInvalidError(path,msg="Can't list directory contents of a file: %(path)s")
            raise OperationFailedError("list directory", path=path, details=e, msg="Unable to get directory listing: %(path)s - (%(details)s)")
        return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)

    def makedir(self, path, recursive=False, allow_recreate=False):
        sys_path = self.getsyspath(path)
        try:
            if recursive:
                os.makedirs(sys_path, self.dir_mode)
            else:
                os.mkdir(sys_path, self.dir_mode)
        except OSError, e:
            if e.errno == 17 or e.errno == 183:
                if self.isfile(path):
                    raise ResourceInvalidError(path,msg="Cannot create directory, there's already a file of that name: %(path)s")
                if not allow_recreate:
                    raise DestinationExistsError(path,msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
            elif e.errno == 2:
                raise ParentDirectoryMissingError(path)
            elif e.errno == 22:
                raise ResourceInvalidError(path)
            else:
                raise OperationFailedError("make directory",path=path,details=e)
                
    def remove(self, path):
        sys_path = self.getsyspath(path)
        try:
            os.remove(sys_path)
        except OSError, e:
            if not self.exists(path):
                raise ResourceNotFoundError(path)
            if self.isdir(path):
                raise ResourceInvalidError(path,msg="Cannot use remove() on a directory: %(path)s")
            raise OperationFailedError("remove file", path=path, details=e)

    def removedir(self, path, recursive=False,force=False):
        sys_path = self.getsyspath(path)
        #  Don't remove the root directory of this FS
        if path in ("","/"):
            return
        if force:
            for path2 in self.listdir(path,absolute=True,files_only=True):
                self.remove(path2)
            for path2 in self.listdir(path,absolute=True,dirs_only=True):
                self.removedir(path2,force=True)
        try:
            os.rmdir(sys_path)
        except OSError, e:
            if self.isfile(path):
                raise ResourceInvalidError(path,msg="Can't use removedir() on a file: %(path)s")
            if self.listdir(path):
                raise DirectoryNotEmptyError(path)
            raise OperationFailedError("remove directory", path=path, details=e)
        #  Using os.removedirs() for this can result in dirs being
        #  removed outside the root of this FS, so we recurse manually.
        if recursive:
            try:
                self.removedir(dirname(path),recursive=True)
            except DirectoryNotEmptyError:
                pass

    def rename(self, src, dst):
        if not issamedir(src, dst):
            raise ValueError("Destination path must the same directory (user the move method for moving to a different directory)")
        path_src = self.getsyspath(src)
        path_dst = self.getsyspath(dst)
        try:
            os.rename(path_src, path_dst)
        except OSError, e:
            raise OperationFailedError("rename resource", path=src, details=e)

    def getinfo(self, path):
        sys_path = self.getsyspath(path)
        try:
            stats = os.stat(sys_path)
        except OSError, e:
            raise ResourceError(path, details=e)
        info = dict((k, getattr(stats, k)) for k in dir(stats) if not k.startswith('__') )
        info['size'] = info['st_size']
        ct = info.get('st_ctime', None)
        if ct is not None:
            info['created_time'] = datetime.datetime.fromtimestamp(ct)
        at = info.get('st_atime', None)
        if at is not None:
            info['accessed_time'] = datetime.datetime.fromtimestamp(at)
        mt = info.get('st_mtime', None)
        if mt is not None:
            info['modified_time'] = datetime.datetime.fromtimestamp(at)
        return info


    def getsize(self, path):
        sys_path = self.getsyspath(path)
        try:
            stats = os.stat(sys_path)
        except OSError, e:
            raise ResourceError(path, details=e)
        return stats.st_size


    #  Provide native xattr support if available
    if xattr:
        def setxattr(self, path, key, value):
            try:
                xattr.xattr(self.getsyspath(path))[key]=value
            except IOError, e:
                raise OperationFailedError('set extended attribute', path=path, details=e)

        def getxattr(self, path, key, default=None):
            try:
                return xattr.xattr(self.getsyspath(path)).get(key)
            except KeyError:
                return default
            except IOError, e:
                raise OperationFailedError('get extended attribute', path=path, details=e)

        def delxattr(self, path, key):
            try:
                del xattr.xattr(self.getsyspath(path))[key]
            except KeyError:
                pass
            except IOError, e:
                raise OperationFailedError('delete extended attribute', path=path, details=e)

        def xattrs(self, path):
            try:
                return xattr.xattr(self.getsyspath(path)).keys()
            except IOError, e:
                raise OperationFailedError('list extended attributes', path=path, details=e)

