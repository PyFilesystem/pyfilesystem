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
import os.path
from os.path import exists as _exists, isdir as _isdir, isfile as _isfile
import sys
import errno
import datetime
import platform
import io
import shutil

scandir = None
try:
    scandir = os.scandir
except AttributeError:
    try:
        from scandir import scandir
    except ImportError:
        pass

from fs.base import *
from fs.path import *
from fs.errors import *
from fs import _thread_synchronize_default

from fs.osfs.xattrs import OSFSXAttrMixin
from fs.osfs.watch import OSFSWatchMixin


@convert_os_errors
def _os_stat(path):
    """Replacement for os.stat that raises FSError subclasses."""
    return os.stat(path)


@convert_os_errors
def _os_mkdir(name, mode=0777):
    """Replacement for os.mkdir that raises FSError subclasses."""
    return os.mkdir(name, mode)


@convert_os_errors
def _os_makedirs(name, mode=0777):
    """Replacement for os.makdirs that raises FSError subclasses.

    This implementation also correctly handles win32 long filenames (those
    starting with "\\\\?\\") which can confuse os.makedirs().  The difficulty
    is that a long-name drive reference like "\\\\?\\C:\\" must end with a
    backslash to be considered a valid path, but os.makedirs() strips them.
    """
    head, tail = os.path.split(name)
    while not tail:
        head, tail = os.path.split(head)
    if sys.platform == "win32" and len(head) == 6:
        if head.startswith("\\\\?\\"):
            head = head + "\\"
    if head and tail and not os.path.exists(head):
        try:
            _os_makedirs(head, mode)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        if tail == os.curdir:
            return
    os.mkdir(name, mode)


class OSFS(OSFSXAttrMixin, OSFSWatchMixin, FS):
    """Expose the underlying operating-system filesystem as an FS object.

    This is the most basic of filesystems, which simply shadows the underlaying
    filesystem of the OS.  Most of its methods simply defer to the matching
    methods in the os and os.path modules.
    """

    _meta = {'thread_safe': True,
             'network': False,
             'virtual': False,
             'read_only': False,
             'unicode_paths': os.path.supports_unicode_filenames,
             'case_insensitive_paths': os.path.normcase('Aa') == 'aa',
             'atomic.makedir': True,
             'atomic.rename': True,
             'atomic.setcontents': False}

    if platform.system() == 'Windows':
        _meta["invalid_path_chars"] = ''.join(chr(n) for n in xrange(31)) + '\\:*?"<>|'
    else:
        _meta["invalid_path_chars"] = '\0'

    def __init__(self, root_path, thread_synchronize=_thread_synchronize_default, encoding=None, create=False, dir_mode=0700, use_long_paths=True):
        """
        Creates an FS object that represents the OS Filesystem under a given root path

        :param root_path: The root OS path
        :param thread_synchronize: If True, this object will be thread-safe by use of a threading.Lock object
        :param encoding: The encoding method for path strings
        :param create: If True, then root_path will be created if it doesn't already exist
        :param dir_mode: The mode to use when creating the directory

        """

        super(OSFS, self).__init__(thread_synchronize=thread_synchronize)
        self.encoding = encoding or sys.getfilesystemencoding() or 'utf-8'
        self.dir_mode = dir_mode
        self.use_long_paths = use_long_paths
        root_path = os.path.expanduser(os.path.expandvars(root_path))
        root_path = os.path.normpath(os.path.abspath(root_path))
        #  Enable long pathnames on win32
        if sys.platform == "win32":
            if use_long_paths and not root_path.startswith("\\\\?\\"):
                if not root_path.startswith("\\"):
                    root_path = u"\\\\?\\" + root_path
                else:
                    # Explicitly mark UNC paths, seems to work better.
                    if root_path.startswith("\\\\"):
                        root_path = u"\\\\?\\UNC\\" + root_path[2:]
                    else:
                        root_path = u"\\\\?" + root_path
            #  If it points at the root of a drive, it needs a trailing slash.
            if len(root_path) == 6 and not root_path.endswith("\\"):
                root_path = root_path + "\\"

        if create:
            try:
                _os_makedirs(root_path, mode=dir_mode)
            except (OSError, DestinationExistsError):
                pass

        if not os.path.exists(root_path):
            raise ResourceNotFoundError(root_path, msg="Root directory does not exist: %(path)s")
        if not os.path.isdir(root_path):
            raise ResourceInvalidError(root_path, msg="Root path is not a directory: %(path)s")
        self.root_path = root_path
        self.dir_mode = dir_mode

    def __str__(self):
        return "<OSFS: %s>" % self.root_path

    def __repr__(self):
        return "<OSFS: %r>" % self.root_path

    def __unicode__(self):
        return u"<OSFS: %s>" % self.root_path

    def _decode_path(self, p):
        if isinstance(p, unicode):
            return p
        return p.decode(self.encoding, 'replace')

    def getsyspath(self, path, allow_none=False):
        self.validatepath(path)
        path = relpath(normpath(path)).replace(u"/", os.sep)
        path = os.path.join(self.root_path, path)
        if not path.startswith(self.root_path):
            raise PathError(path, msg="OSFS given path outside root: %(path)s")
        path = self._decode_path(path)
        return path

    def unsyspath(self, path):
        """Convert a system-level path into an FS-level path.

        This basically the reverse of getsyspath().  If the path does not
        refer to a location within this filesystem, ValueError is raised.

        :param path: a system path
        :returns: a path within this FS object
        :rtype: string

        """
        # TODO: HAve a closer look at this method
        path = os.path.normpath(os.path.abspath(path))
        path = self._decode_path(path)
        if sys.platform == "win32":
            if len(path) == 6 and not path.endswith("\\"):
                path = path + "\\"
        prefix = os.path.normcase(self.root_path)
        if not prefix.endswith(os.path.sep):
            prefix += os.path.sep
        if not os.path.normcase(path).startswith(prefix):
            raise ValueError("path not within this FS: %s (%s)" % (os.path.normcase(path), prefix))
        return normpath(path[len(self.root_path):])

    def getmeta(self, meta_name, default=NoDefaultMeta):

        if meta_name == 'free_space':
            if platform.system() == 'Windows':
                try:
                    import ctypes
                    free_bytes = ctypes.c_ulonglong(0)
                    ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(self.root_path), None, None, ctypes.pointer(free_bytes))
                    return free_bytes.value
                except ImportError:
                    # Fall through to call the base class
                    pass
            else:
                stat = os.statvfs(self.root_path)
                return stat.f_bfree * stat.f_bsize
        elif meta_name == 'total_space':
            if platform.system() == 'Windows':
                try:
                    import ctypes
                    total_bytes = ctypes.c_ulonglong(0)
                    ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(self.root_path), None, ctypes.pointer(total_bytes), None)
                    return total_bytes.value
                except ImportError:
                    # Fall through to call the base class
                    pass
            else:
                stat = os.statvfs(self.root_path)
                return stat.f_blocks * stat.f_bsize

        return super(OSFS, self).getmeta(meta_name, default)

    @convert_os_errors
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        mode = ''.join(c for c in mode if c in 'rwabt+')
        sys_path = self.getsyspath(path)
        if not encoding and 'b' not in mode:
            encoding = encoding or 'utf-8'
        try:
            return io.open(sys_path, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        except EnvironmentError, e:
            #  Win32 gives EACCES when opening a directory.
            if sys.platform == "win32" and e.errno in (errno.EACCES,):
                if self.isdir(path):
                    raise ResourceInvalidError(path)
            raise

    @convert_os_errors
    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=64 * 1024):
        return super(OSFS, self).setcontents(path, data, encoding=encoding, errors=errors, chunk_size=chunk_size)

    @convert_os_errors
    def exists(self, path):
        return _exists(self.getsyspath(path))

    @convert_os_errors
    def isdir(self, path):
        return _isdir(self.getsyspath(path))

    @convert_os_errors
    def isfile(self, path):
        return _isfile(self.getsyspath(path))

    @convert_os_errors
    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        _decode_path = self._decode_path
        sys_path = self.getsyspath(path)

        if scandir is None:
            listing = os.listdir(sys_path)
            paths = [_decode_path(p) for p in listing]
            return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)
        else:
            if dirs_only and files_only:
                raise ValueError("dirs_only and files_only can not both be True")
            # Use optimized scandir if present
            scan = scandir(sys_path)
            if dirs_only:
                paths = [_decode_path(dir_entry.name) for dir_entry in scan if dir_entry.is_dir()]
            elif files_only:
                paths = [_decode_path(dir_entry.name) for dir_entry in scan if dir_entry.is_file()]
            else:
                paths = [_decode_path(dir_entry.name) for dir_entry in scan]

            return self._listdir_helper(path, paths, wildcard, full, absolute, False, False)

    @convert_os_errors
    def makedir(self, path, recursive=False, allow_recreate=False):
        sys_path = self.getsyspath(path)
        try:
            if recursive:
                _os_makedirs(sys_path, self.dir_mode)
            else:
                _os_mkdir(sys_path, self.dir_mode)
        except DestinationExistsError:
            if self.isfile(path):
                raise ResourceInvalidError(path, msg="Cannot create directory, there's already a file of that name: %(path)s")
            if not allow_recreate:
                raise DestinationExistsError(path, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")
        except ResourceNotFoundError:
            raise ParentDirectoryMissingError(path)

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
            if e.errno == errno.EPERM and sys.platform == "darwin":
                # sometimes OSX says this for attempts to remove a dir
                if os.path.isdir(sys_path):
                    raise ResourceInvalidError(path)
            raise

    @convert_os_errors
    def removedir(self, path, recursive=False, force=False):
        #  Don't remove the root directory of this FS
        if path in ('', '/'):
            raise RemoveRootError(path)
        sys_path = self.getsyspath(path)
        if force:
            # shutil implementation handles concurrency better
            shutil.rmtree(sys_path, ignore_errors=True)
        else:
            os.rmdir(sys_path)
        #  Using os.removedirs() for this can result in dirs being
        #  removed outside the root of this FS, so we recurse manually.
        if recursive:
            try:
                if dirname(path) not in ('', '/'):
                    self.removedir(dirname(path), recursive=True)
            except DirectoryNotEmptyError:
                pass

    @convert_os_errors
    def rename(self, src, dst):
        path_src = self.getsyspath(src)
        path_dst = self.getsyspath(dst)
        try:
            os.rename(path_src, path_dst)
        except OSError, e:
            if e.errno:
                #  POSIX rename() can rename over an empty directory but gives
                #  ENOTEMPTY if the dir has contents.  Raise UnsupportedError
                #  instead of DirectoryEmptyError in this case.
                if e.errno == errno.ENOTEMPTY:
                    raise UnsupportedError("rename")
                #  Linux (at least) gives ENOENT when trying to rename into
                #  a directory that doesn't exist.  We want ParentMissingError
                #  in this case.
                if e.errno == errno.ENOENT:
                    if not os.path.exists(os.path.dirname(path_dst)):
                        raise ParentDirectoryMissingError(dst)
            raise

    def _stat(self, path):
        """Stat the given path, normalising error codes."""
        sys_path = self.getsyspath(path)
        try:
            return _os_stat(sys_path)
        except ResourceInvalidError:
            raise ResourceNotFoundError(path)

    @convert_os_errors
    def getinfo(self, path):
        stats = self._stat(path)
        info = dict((k, getattr(stats, k)) for k in dir(stats) if k.startswith('st_'))
        info['size'] = info['st_size']
        #  TODO: this doesn't actually mean 'creation time' on unix
        fromtimestamp = datetime.datetime.fromtimestamp
        ct = info.get('st_ctime', None)
        if ct is not None:
            info['created_time'] = fromtimestamp(ct)
        at = info.get('st_atime', None)
        if at is not None:
            info['accessed_time'] = fromtimestamp(at)
        mt = info.get('st_mtime', None)
        if mt is not None:
            info['modified_time'] = fromtimestamp(mt)
        return info

    @convert_os_errors
    def getinfokeys(self, path, *keys):
        info = {}
        stats = self._stat(path)
        fromtimestamp = datetime.datetime.fromtimestamp
        for key in keys:
            try:
                if key == 'size':
                    info[key] = stats.st_size
                elif key == 'modified_time':
                    info[key] = fromtimestamp(stats.st_mtime)
                elif key == 'created_time':
                    info[key] = fromtimestamp(stats.st_ctime)
                elif key == 'accessed_time':
                    info[key] = fromtimestamp(stats.st_atime)
                else:
                    info[key] = getattr(stats, key)
            except AttributeError:
                continue
        return info


    @convert_os_errors
    def getsize(self, path):
        return self._stat(path).st_size
