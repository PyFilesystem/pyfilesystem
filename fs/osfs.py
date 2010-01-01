"""
fs.osfs
=======

Exposes the OS Filesystem as an FS object.

For example, to print all the files and directories in the OS root::

    >>> from fs.osfs import OSFS
    >>> home_fs = OSFS('/')
    >>> print home_fs.listdir()

"""


import os
import sys
import errno

from fs.base import *
from fs.path import *
from fs import _thread_syncronize_default

try:
    import xattr
except ImportError:
    xattr = None


@convert_os_errors
def _os_stat(path):
    """Replacement for os.stat that raises FSError subclasses."""
    return os.stat(path)


class OSFS(FS):
    """Expose the underlying operating-system filesystem as an FS object.

    This is the most basic of filesystems, which simply shadows the underlaying
    filesytem of the OS.  Most of its methods simply defer to the corresponding
    methods in the os and os.path modules.
    """

    def __init__(self, root_path, dir_mode=0700, thread_synchronize=_thread_syncronize_default, encoding=None):
        
        """
        Creates an FS object that represents the OS Filesystem under a given root path
        
        :param root_path: The root OS path
        :param dir_mode: srt
        :param thread_syncronize: If True, this object will be thread-safe by use of a threading.Lock object
        :param encoding: The encoding method for path strings
        
        """
        
        FS.__init__(self, thread_synchronize=thread_synchronize)
        self.encoding = encoding
        root_path = os.path.expanduser(os.path.expandvars(root_path))
        root_path = os.path.normpath(os.path.abspath(root_path))
        #  Enable long pathnames on win32
        if sys.platform == "win32":
            if not root_path.startswith("\\\\?\\"):
                root_path = u"\\\\?\\" + root_path
        if not os.path.exists(root_path):
            raise ResourceNotFoundError(root_path,msg="Root directory does not exist: %(path)s")
        if not os.path.isdir(root_path):
            raise ResourceInvalidError(root_path,msg="Root path is not a directory: %(path)s")
        self.root_path = root_path
        self.dir_mode = dir_mode

    def __str__(self):
        return "<OSFS: %s>" % self.root_path
    
    def __unicode__(self):
        return u"<OSFS: %s>" % self.root_path

    def getsyspath(self, path, allow_none=False):
        path = relpath(normpath(path)).replace("/",os.sep)
        path = os.path.join(self.root_path, path)
        if not isinstance(path,unicode):
            if self.encoding is None:
                path = path.decode(sys.getfilesystemencoding())
            else:
                path = path.decode(self.encoding)
        return path

    @convert_os_errors
    def open(self, path, mode="r", **kwargs):
        mode = filter(lambda c: c in "rwabt+",mode)
        return open(self.getsyspath(path), mode, kwargs.get("buffering", -1))

    @convert_os_errors
    def exists(self, path):
        path = self.getsyspath(path)
        return os.path.exists(path)

    @convert_os_errors
    def isdir(self, path):
        path = self.getsyspath(path)
        return os.path.isdir(path)

    @convert_os_errors
    def isfile(self, path):
        path = self.getsyspath(path)
        return os.path.isfile(path)

    @convert_os_errors
    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        paths = os.listdir(self.getsyspath(path))
        return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)

    @convert_os_errors
    def makedir(self, path, recursive=False, allow_recreate=False):
        sys_path = self.getsyspath(path)
        try:
            if recursive:
                os.makedirs(sys_path, self.dir_mode)
            else:
                os.mkdir(sys_path, self.dir_mode)
        except OSError, e:
            if e.errno == errno.EEXIST or e.errno == 183:
                if self.isfile(path):
                    raise ResourceInvalidError(path,msg="Cannot create directory, there's already a file of that name: %(path)s")
                if not allow_recreate:
                    raise DestinationExistsError(path,msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
            elif e.errno == errno.ENOENT:
                raise ParentDirectoryMissingError(path)
            else:
                raise
                
    @convert_os_errors
    def remove(self, path):
        sys_path = self.getsyspath(path)
        try:
            os.remove(sys_path)
        except OSError, e:
            if e.errno == errno.EACCES and sys.platform == "win32":
                # sometimes windows says this for attempts to remove a dir
                if os.path.isdir(sys_path):
                    raise ResourceInvalidError(path)
            raise

    @convert_os_errors
    def removedir(self, path, recursive=False, force=False):
        sys_path = self.getsyspath(path)
        if force:
            for path2 in self.listdir(path, absolute=True, files_only=True):
                try:
                    self.remove(path2)
                except ResourceNotFoundError:
                    pass
            for path2 in self.listdir(path, absolute=True, dirs_only=True):
                try:
                    self.removedir(path2, force=True)
                except ResourceNotFoundError:
                    pass
        #  Don't remove the root directory of this FS
        if path in ("","/"):
            return
        os.rmdir(sys_path)
        #  Using os.removedirs() for this can result in dirs being
        #  removed outside the root of this FS, so we recurse manually.
        if recursive:
            try:
                self.removedir(dirname(path),recursive=True)
            except DirectoryNotEmptyError:
                pass

    @convert_os_errors
    def rename(self, src, dst):
        path_src = self.getsyspath(src)
        path_dst = self.getsyspath(dst)
        os.rename(path_src, path_dst)

    def _stat(self,path):
        """Stat the given path, normalising error codes."""
        sys_path = self.getsyspath(path)
        try:
            return _os_stat(sys_path)
        except ResourceInvalidError:
            raise ResourceNotFoundError(path)

    @convert_os_errors
    def getinfo(self, path):
        stats = self._stat(path)
        info = dict((k, getattr(stats, k)) for k in dir(stats) if not k.startswith('__') )
        info['size'] = info['st_size']
        #  TODO: this doesn't actually mean 'creation time' on unix
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

    @convert_os_errors
    def getsize(self, path):
        return self._stat(path).st_size


    #  Provide native xattr support if available
    if xattr:
        @convert_os_errors
        def setxattr(self, path, key, value):
            xattr.xattr(self.getsyspath(path))[key]=value

        @convert_os_errors
        def getxattr(self, path, key, default=None):
            try:
                return xattr.xattr(self.getsyspath(path)).get(key)
            except KeyError:
                return default

        @convert_os_errors
        def delxattr(self, path, key):
            try:
                del xattr.xattr(self.getsyspath(path))[key]
            except KeyError:
                pass

        @convert_os_errors
        def listxattrs(self, path):
            return xattr.xattr(self.getsyspath(path)).keys()


