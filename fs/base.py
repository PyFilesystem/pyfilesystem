#!/usr/bin/env python
"""

  fs.base:  base class defining the FS abstraction.

This module defines the most basic filesystem abstraction, the FS class.
Instances of FS represent a filesystem containing files and directories
that can be queried and manipulated.  To implement a new kind of filesystem,
start by sublcassing the base FS class.

"""

__all__ = ['DummyLock',
           'silence_fserrors',
           'NullFile',
           'synchronize',
           'FS',
           'flags_to_mode']

import os, os.path
import sys
import shutil
import fnmatch
import datetime
import time
import re
try:
    import threading
except ImportError:
    import dummy_threading as threading

from fs.path import *
from fs.errors import *
from fs.functools import wraps


class DummyLock(object):
    """A dummy lock object that doesn't do anything.

    This is used as a placeholder when locking is disabled.  We can't
    directly use the Lock class from the dummy_threading module, since
    it attempts to sanity-check the sequence of acquire/release calls
    in a way that breaks when real threading is available.
    """

    def acquire(self,blocking=1):
        """Acquiring a DummyLock always succeeds."""
        return 1

    def release(self):
        """Releasing a DummyLock always succeeds."""
        pass


def silence_fserrors(f, *args, **kwargs):
    """Perform a function call and return None if FSError is thrown

    :param f: Function to call
    :param args: Parameters to f
    :param kwargs: Keyword parameters to f

    """
    try:
        return f(*args, **kwargs)
    except FSError:
        return None


class NullFile(object):
    """A NullFile is a file object that has no functionality.

    Null files are returned by the 'safeopen' method in FS objects when the
    file doesn't exist. This can simplify code by negating the need to check
    if a file exists, or handling exceptions.

    """
    def __init__(self):
        self.closed = False

    def flush(self):
        pass

    def __iter__(self):
        return self

    def next(self):
        raise StopIteration

    def readline(self, *args, **kwargs):
        return ""

    def close(self):
        self.closed = True

    def read(self, size=None):
        return ""

    def seek(self, *args, **kwargs):
        pass

    def tell(self):
        return 0

    def truncate(self, *args, **kwargs):
        return 0

    def write(self, data):
        pass

    def writelines(self, *args, **kwargs):
        pass


def synchronize(func):
    """Decorator to synchronize a method on self._lock."""
    @wraps(func)
    def acquire_lock(self, *args, **kwargs):
        self._lock.acquire()
        try:
            return func(self, *args, **kwargs)
        finally:
            self._lock.release()
    return acquire_lock


class FS(object):
    """The base class for Filesystem abstraction objects.

    An instance of a class derived from FS is an abstraction on some kind
    of filesytem, such as the OS filesystem or a zip file.

    """

    def __init__(self, thread_synchronize=False):
        """The base class for Filesystem objects.

        :param thread_synconize: If True, a lock object will be created for the object, otherwise a dummy lock will be used.
        :type thread_synchronize: bool
        """
        super(FS,self).__init__()
        self.closed = False
        if thread_synchronize:
            self._lock = threading.RLock()
        else:
            self._lock = DummyLock()

    def __del__(self):
        if not getattr(self, 'closed', True):
            self.close()

    def cache_hint(self, enabled):
        """Recommends the use of caching. Implementations are free to use or
            ignore this value.

        :param enabled: If True the implementation is permitted to cache directory
            structure / file info.

        """
        pass

    def close(self):
        self.closed = True

    def __getstate__(self):
        #  Locks can't be pickled, so instead we just indicate the
        #  type of lock that should be there.  None == no lock,
        #  True == a proper lock, False == a dummy lock.
        state = self.__dict__.copy()
        lock = state.get("_lock",None)
        if lock is not None:
            if isinstance(lock,threading._RLock):
                state["_lock"] = True
            else:
                state["_lock"] = False
        return state

    def __setstate__(self,state):
        for (k,v) in state.iteritems():
            self.__dict__[k] = v
        lock = state.get("_lock",None)
        if lock is not None:
            if lock:
                self._lock = threading.RLock()
            else:
                self._lock = DummyLock()

    def getsyspath(self, path, allow_none=False):
        """Returns the system path (a path recognised by the OS) if present.

        If the path does not map to a system path (and allow_none is False)
        then a NoSysPathError exception is thrown.  Otherwise, the system
        path will be returned as a unicode string.

        :param path: a path within the filesystem
        :param allow_none: if True, this method will return None when there is no system path,
            rather than raising NoSysPathError
        :type allow_none: bool
        :raises NoSysPathError: If the path does not map on to a system path, and allow_none is set to False (default)
        :rtype: unicode
        """
        if not allow_none:
            raise NoSysPathError(path=path)
        return None

    def hassyspath(self, path):
        """Check if the path maps to a system path (a path recognised by the OS).

        :param path: -- path to check
        :returns: True if `path` maps to a system path
        :rtype: bool        
        """
        return self.getsyspath(path, allow_none=True) is not None

    def open(self, path, mode="r", **kwargs):
        """Open a the given path as a file-like object.

        :param path: a path to file that should be opened
        :param mode: ,ode of file to open, identical to the mode string used
            in 'file' and 'open' builtins
        :param kwargs: additional (optional) keyword parameters that may
            be required to open the file        
        :rtype: a file-like object
        """
        raise UnsupportedError("open file")

    def safeopen(self, *args, **kwargs):
        """Like 'open', but returns a NullFile if the file could not be opened.

        A NullFile is a dummy file which has all the methods of a file-like object,
        but contains no data.

        :rtype: file-like object

        """
        try:
            f = self.open(*args, **kwargs)
        except ResourceNotFoundError:
            return NullFile()
        return f

    def exists(self, path):
        """Check if a path references a valid resource.

        :param path: A path in the filessystem
        :rtype: bool
        """
        return self.isfile(path) or self.isdir(path)

    def isdir(self, path):
        """Check if a path references a directory.

        :param path: a path in the filessystem
        :rtype: bool

        """
        raise UnsupportedError("check for directory")

    def isfile(self, path):
        """Check if a path references a file.

        :param path: a path in the filessystem
        :rtype: bool

        """
        raise UnsupportedError("check for file")

    def __iter__(self):
        """ Iterates over paths returned by listdir method with default params. """
        for f in self.listdir():
            yield f

    def listdir(self,   path="./",
                        wildcard=None,
                        full=False,
                        absolute=False,
                        dirs_only=False,
                        files_only=False):
        """Lists the the files and directories under a given path.

        The directory contents are returned as a list of unicode paths.

        :param path: root of the path to list
        :type path: string
        :param wildcard: Only returns paths that match this wildcard
        :type wildcard: string containing a wildcard, or a callable that accepts a path and returns a boolean
        :param full: returns full paths (relative to the root)
        :type full: bool
        :param absolute: returns absolute paths (paths begining with /)
        :type absolute: bool
        :param dirs_only: if True, only return directories
        :type dirs_only: bool
        :param files_only: if True, only return files
        :type files_only: bool        
        :rtype: iterable of paths

        :raises ResourceNotFoundError: if the path is not found
        :raises ResourceInvalidError: if the path exists, but is not a directory

        """
        raise UnsupportedError("list directory")

    def listdirinfo(self, path="./",
                          wildcard=None,
                          full=False,
                          absolute=False,
                          dirs_only=False,
                          files_only=False):

        """Retrieves an iterable of paths and path info (as returned by getinfo) under
        a given path.

        :param path: root of the path to list
        :param wildcard: filter paths that match this wildcard
        :param dirs_only: only retrive directories
        :type dirs_only: bool
        :param files_only: only retrieve files
        :type files_only: bool

        :raises ResourceNotFoundError: If the path is not found
        :raises ResourceInvalidError: If the path exists, but is not a directory

        """

        def get_path(p):
            if not full or absolute:
                return pathjoin(path, p)

        def getinfo(p):
            try:
                return self.getinfo(get_path(p))
            except FSError:
                return {}

        return [(p, getinfo(get_path(p)))
                    for p in self.listdir(path,                                          
                                          wildcard=wildcard,
                                          full=full,
                                          absolute=absolute,
                                          dirs_only=dirs_only,
                                          files_only=files_only)]

    def _listdir_helper(self, path, entries,
                              wildcard=None,
                              full=False,
                              absolute=False,
                              dirs_only=False,
                              files_only=False):
        """A helper method called by listdir method that applies filtering.

        Given the path to a directory and a list of the names of entries within
        that directory, this method applies the semantics of the listdir()
        keyword arguments.  An appropriately modified and filtered list of
        directory entries is returned.
        """
        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only can not both be True")

        if wildcard is not None:
            if not callable(wildcard):
                wildcard_re = re.compile(fnmatch.translate(wildcard))
                wildcard = lambda fn:bool (wildcard_re.match(fn))          
            entries = [p for p in entries if wildcard(p)]

        if dirs_only:
            entries = [p for p in entries if self.isdir(pathjoin(path, p))]
        elif files_only:
            entries = [p for p in entries if self.isfile(pathjoin(path, p))]

        if full:
            entries = [pathjoin(path, p) for p in entries]
        elif absolute:
            entries = [abspath(pathjoin(path, p)) for p in entries]

        return entries

    def makedir(self, path, recursive=False, allow_recreate=False):
        """Make a directory on the filesystem.

        :param path: path of directory
        :param recursive: if True, any intermediate directories will also be created
        :type recursive: bool
        :param allow_recreate: if True, re-creating a directory wont be an error
        :type allow_create: bool

        :raises DestinationExistsError: if the path is already a directory, and allow_recreate is False
        :raises ParentDirectoryMissingError: if a containing directory is missing and recursive is False
        :raises ResourceInvalidError: if a path is an existing file

        """
        raise UnsupportedError("make directory")

    def remove(self, path):
        """Remove a file from the filesystem.

        :param path: Path of the resource to remove

        :raises ResourceNotFoundError: if the path does not exist
        :raises ResourceInvalidError: if the path is a directory

        """
        raise UnsupportedError("remove resource")

    def removedir(self, path, recursive=False, force=False):
        """Remove a directory from the filesystem

        :param path: path of the directory to remove
        :param recursive: pf True, then empty parent directories will be removed
        :type recursive: bool
        :param force: if True, any directory contents will be removed
        :type force: bool

        :raises ResourceNotFoundError: If the path does not exist
        :raises ResourceInvalidError: If the path is not a directory
        :raises DirectoryNotEmptyError: If the directory is not empty and force is False

        """
        raise UnsupportedError("remove directory")

    def rename(self, src, dst):
        """Renames a file or directory

        :param src: path to rename
        :param dst: new name
        """
        raise UnsupportedError("rename resource")

    @convert_os_errors
    def settimes(self, path, accessed_time=None, modified_time=None):
        """Set the accessed time and modified time of a file
        
        :param path: path to a file
        :param accessed_time: a datetime object the file was accessed (defaults to current time) 
        :param modified_time: a datetime object the file was modified (defaults to current time)
        
        """
                
        sys_path = self.getsyspath(path, allow_none=True)
        if sys_path is not None:            
            now = datetime.datetime.now()
            if accessed_time is None:
                accessed_time = now
            if modified_time is None:
                modified_time = now                         
            accessed_time = int(time.mktime(accessed_time.timetuple()))
            modified_time = int(time.mktime(modified_time.timetuple()))
            os.utime(sys_path, (accessed_time, modified_time))
            return True
        else:
            raise UnsupportedError("settimes")
                   
    def getinfo(self, path):
        """Returns information for a path as a dictionary. The exact content of
        this dictionary will vary depending on the implementation, but will
        likely include a few common values.

        :param path: a path to retrieve information for
        :rtype: dict
        """
        raise UnsupportedError("get resource info")

    def desc(self, path):
        """Returns short descriptive text regarding a path. Intended mainly as
        a debugging aid

        :param path: A path to describe
        :rtype: str
        
        """
        if not self.exists(path):
            return ''
        try:
            sys_path = self.getsyspath(path)
        except NoSysPathError:
            return "No description available"
        if self.isdir(path):
            return "OS dir, maps to %s" % sys_path
        else:
            return "OS file, maps to %s" % sys_path

    def getcontents(self, path):
        """Returns the contents of a file as a string.

        :param path: A path of file to read
        :rtype: str
        :returns: file contents
        """
        f = None
        try:
            f = self.open(path, "rb")
            contents = f.read()
            return contents
        finally:
            if f is not None:
                f.close()

    def createfile(self, path, data=""):
        """A convenience method to create a new file from a string.

        :param path: a path of the file to create
        :param data: a string or a file-like object containing the contents for the new file
        """
        f = None
        try:
            f = self.open(path, 'wb')
            if hasattr(data, "read"):
                chunk = data.read(1024*512)
                while chunk:
                    f.write(chunk)
                    chunk = data.read(1024*512)
            else:
                f.write(data)
            f.flush()
        finally:
            if f is not None:
                f.close()
    setcontents = createfile

    def opendir(self, path):
        """Opens a directory and returns a FS object representing its contents.

        :param path: path to directory to open
        :rtype: An FS object
        """
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        sub_fs = SubFS(self, path)
        return sub_fs

    def walk(self,
             path="/",
             wildcard=None,
             dir_wildcard=None,
             search="breadth",
             ignore_errors=False):
        """Walks a directory tree and yields the root path and contents.
        Yields a tuple of the path of each directory and a list of its file
        contents.

        :param path: root path to start walking
        :param wildcard: if given, only return files that match this wildcard
        :type wildcard: A string containing a wildcard (e.g. *.txt) or a callable that takes the file path and returns a boolean
        :param dir_wildcard: if given, only walk directories that match the wildcard
        :type dir_wildcard: A string containing a wildcard (e.g. *.txt) or a callable that takes the directory name and returns a boolean
        :param search: -- a string dentifying the method used to walk the directories. There are two such methods:
            * 'breadth' Yields paths in the top directories first
            * 'depth' Yields the deepest paths first
        :param ignore_errors: ignore any errors reading the directory

        """
        
        
        if wildcard is None:
            wildcard = lambda f:True
        elif not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn:bool (wildcard_re.match(fn))
            
        if dir_wildcard is None:
            dir_wildcard = lambda f:True
        elif not callable(dir_wildcard):
            dir_wildcard_re = re.compile(fnmatch.translate(dir_wildcard))
            dir_wildcard = lambda fn:bool (dir_wildcard_re.match(fn))                    
        
        def listdir(path, *args, **kwargs):
            if ignore_errors:
                try:
                    return self.listdir(path, *args, **kwargs)
                except:
                    return []
            else:
                return self.listdir(path, *args, **kwargs)
        
        if search == "breadth":
            
            dirs = [path]
            while dirs:
                current_path = dirs.pop()
                paths = []
                for filename in listdir(current_path):
                    path = pathjoin(current_path, filename)
                    if self.isdir(path):                        
                        if dir_wildcard(path):
                            dirs.append(path)                        
                    else:                        
                        if wildcard(filename):
                            paths.append(filename)
                        
                yield (current_path, paths)

        elif search == "depth":

            def recurse(recurse_path):
                for path in listdir(recurse_path, wildcard=dir_wildcard, full=True, dirs_only=True):
                    for p in recurse(path):
                        yield p
                yield (recurse_path, self.listdir(recurse_path, wildcard=wildcard, files_only=True))

            for p in recurse(path):
                yield p
                
        else:
            raise ValueError("Search should be 'breadth' or 'depth'")

    def walkfiles(self,
                  path="/",
                  wildcard=None,
                  dir_wildcard=None,
                  search="breadth",
                  ignore_errors=False ):
        """Like the 'walk' method, but just yields file paths.

        :param path: root path to start walking
        :param wildcard: if given, only return files that match this wildcard
        :type wildcard: A string containing a wildcard (e.g. *.txt) or a callable that takes the file path and returns a boolean
        :param dir_wildcard: if given, only walk directories that match the wildcard
        :type dir_wildcard: A string containing a wildcard (e.g. *.txt) or a callable that takes the directory name and returns a boolean
        :param search: same as walk method
        :param ignore_errors: ignore any errors reading the directory
        """
        for path, files in self.walk(path, wildcard=wildcard, dir_wildcard=dir_wildcard, search=search, ignore_errors=ignore_errors):
            for f in files:
                yield pathjoin(path, f)

    def walkdirs(self,
                 path="/",
                 wildcard=None,
                 search="breadth",
                 ignore_errors=False):
        """Like the 'walk' method but yields directories.

        :param path: root path to start walking
        :param wildcard: if given, only return dictories that match this wildcard
        :type wildcard: A string containing a wildcard (e.g. *.txt) or a callable that takes the directory name and returns a boolean
        :param search: same as the walk method
        :param ignore_errors: ignore any errors reading the directory
        """
        for p, files in self.walk(path, wildcard=wildcard, search=search, ignore_errors=ignore_errors):
            yield p

    def getsize(self, path):
        """Returns the size (in bytes) of a resource.

        :param path: a path to the resource
        :rtype: integer
        :returns: the size of the file
        """
        info = self.getinfo(path)
        size = info.get('size', None)
        if size is None:
            raise OperationFailedError("get size of resource", path)
        return size

    def copy(self, src, dst, overwrite=False, chunk_size=16384):
        """Copies a file from src to dst.

        :param src: the source path
        :param dst: the destination path
        :param overwrite: if True, then an existing file at the destination may
            be overwritten; If False then DestinationExistsError
            will be raised.
        :param chunk_size: size of chunks to use if a simple copy is required
            (defaults to 16K).
        """

        if not self.isfile(src):
            if self.isdir(src):
                raise ResourceInvalidError(src,msg="Source is not a file: %(path)s")
            raise ResourceNotFoundError(src)
        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)

        src_syspath = self.getsyspath(src, allow_none=True)
        dst_syspath = self.getsyspath(dst, allow_none=True)

        if src_syspath is not None and dst_syspath is not None:
            self._shutil_copyfile(src_syspath, dst_syspath)
        else:
            src_file, dst_file = None, None
            try:
                src_file = self.open(src, "rb")
                dst_file = self.open(dst, "wb")

                while True:
                    chunk = src_file.read(chunk_size)
                    dst_file.write(chunk)
                    if len(chunk) != chunk_size:
                        break
            finally:
                if src_file is not None:
                    src_file.close()
                if dst_file is not None:
                    dst_file.close()

    @convert_os_errors
    def _shutil_copyfile(self, src_syspath, dst_syspath):
        try:
            shutil.copyfile(src_syspath, dst_syspath)
        except IOError, e:
            #  shutil reports ENOENT when a parent directory is missing
            if getattr(e,"errno",None) == 2:
                if not os.path.exists(dirname(dst_syspath)):
                    raise ParentDirectoryMissingError(dst_syspath)
            raise

    def move(self, src, dst, overwrite=False, chunk_size=16384):
        """moves a file from one location to another.

        :param src: source path
        :param dst: destination path
        :param overwrite: if True, then an existing file at the destination path
            will be silently overwritten; if False then an exception
            will be raised in this case.
        :type overwrite: bool
        :param chunk_size: Size of chunks to use when copying, if a simple copy
            is required
        :type chunk_size: integer
        """

        src_syspath = self.getsyspath(src, allow_none=True)
        dst_syspath = self.getsyspath(dst, allow_none=True)

        #  Try to do an os-level rename if possible.
        #  Otherwise, fall back to copy-and-remove.
        if src_syspath is not None and dst_syspath is not None:
            if not os.path.isfile(src_syspath):
                if os.path.isdir(src_syspath):
                    raise ResourceInvalidError(src, msg="Source is not a file: %(path)s")
                raise ResourceNotFoundError(src)
            if not overwrite and os.path.exists(dst_syspath):
                raise DestinationExistsError(dst)
            try:
                os.rename(src_syspath, dst_syspath)
                return
            except OSError:
                pass
        self.copy(src, dst, overwrite=overwrite, chunk_size=chunk_size)
        self.remove(src)

    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        """moves a directory from one location to another.

        :param src: source directory path
        :param dst: destination directory path
        :param overwrite: if True then any existing files in the destination
            directory will be overwritten
        :param ignore_errors: if True then this method will ignore FSError
            exceptions when moving files
        :param chunk_size: size of chunks to use when copying, if a simple copy
            is required
        """
        if not self.isdir(src):
            raise ResourceInvalidError(src, msg="Source is not a directory: %(path)s")
        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)

        src_syspath = self.getsyspath(src, allow_none=True)
        dst_syspath = self.getsyspath(dst, allow_none=True)

        if src_syspath is not None and dst_syspath is not None:
            try:
                os.rename(src_syspath,dst_syspath)
                return
            except OSError:
                pass

        def movefile_noerrors(src, dst, **kwargs):
            try:
                return self.move(src, dst, **kwargs)
            except FSError:
                return
        if ignore_errors:
            movefile = movefile_noerrors
        else:
            movefile = self.move

        src = abspath(src)
        dst = abspath(dst)

        if dst:
            self.makedir(dst, allow_recreate=overwrite)

        for dirname, filenames in self.walk(src, search="depth"):

            dst_dirname = relpath(frombase(src, abspath(dirname)))
            dst_dirpath = pathjoin(dst, dst_dirname)
            self.makedir(dst_dirpath, allow_recreate=True, recursive=True)

            for filename in filenames:

                src_filename = pathjoin(dirname, filename)
                dst_filename = pathjoin(dst_dirpath, filename)
                movefile(src_filename, dst_filename, overwrite=overwrite, chunk_size=chunk_size)

            self.removedir(dirname)

    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        """copies a directory from one location to another.

        :param src: source directory path
        :param dst: destination directory path
        :param overwrite: if True then any existing files in the destination
            directory will be overwritten
        :type overwrite: bool
        :param ignore_errors: if True, exceptions when copying will be ignored
        :type ignore_errors: bool
        :param chunk_size: size of chunks to use when copying, if a simple copy
            is required (defaults to 16K)
        """
        if not self.isdir(src):
            raise ResourceInvalidError(src, msg="Source is not a directory: %(path)s")
        def copyfile_noerrors(src, dst, **kwargs):
            try:
                return self.copy(src, dst, **kwargs)
            except FSError:
                return
        if ignore_errors:
            copyfile = copyfile_noerrors
        else:
            copyfile = self.copy

        src = abspath(src)
        dst = abspath(dst)

        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)

        if dst:
            self.makedir(dst, allow_recreate=overwrite)

        for dirname, filenames in self.walk(src):

            dst_dirname = relpath(frombase(src, abspath(dirname)))
            dst_dirpath = pathjoin(dst, dst_dirname)
            self.makedir(dst_dirpath, allow_recreate=True, recursive=True)

            for filename in filenames:

                src_filename = pathjoin(dirname, filename)
                dst_filename = pathjoin(dst_dirpath, filename)
                copyfile(src_filename, dst_filename, overwrite=overwrite, chunk_size=chunk_size)

    def isdirempty(self, path):
        """Check if a directory is empty (contains no files or sub-directories)

        :param path: a directory path
        :rtype: bool
        """
        path = normpath(path)
        iter_dir = iter(self.listdir(path))
        try:
            iter_dir.next()
        except StopIteration:
            return True
        return False

    def makeopendir(self, path, recursive=False):
        """makes a directory (if it doesn't exist) and returns an FS object for
        the newly created directory.

        :param path: path to the new directory
        :param recursive: if True any intermediate directories will be created

        """

        self.makedir(path, allow_recreate=True, recursive=recursive)
        dir_fs = self.opendir(path)
        return dir_fs

    def printtree(self, max_levels=5):
        """Prints a tree structure of the FS object to the console
        
        :param max_levels: The maximum sub-directories to display, defaults to
            5. Set to None for no limit 
        
        """
        from fs.utils import print_fs                
        print_fs(self, max_levels=max_levels)
    tree = printtree
    
    def browse(self):
        """Displays the FS tree in a graphical window (requires wxWidgets)"""
        from fs.browsewin import browse
        browse(self)


class SubFS(FS):
    """A SubFS represents a sub directory of another filesystem object.

    SubFS objects are returned by opendir, which effectively creates a
    'sandbox' filesystem that can only access files/dirs under a root path
    within its 'parent' dir.
    """

    def __init__(self, parent, sub_dir):
        self.parent = parent
        self.sub_dir = abspath(normpath(sub_dir))
        FS.__init__(self, thread_synchronize=False)

    def __str__(self):
        return "<SubFS: %s in %s>" % (self.sub_dir, self.parent)

    def __unicode__(self):
        return u"<SubFS: %s in %s>" % (self.sub_dir, self.parent)

    def __repr__(self):
        return str(self)

    def desc(self, path):
        if self.isdir(path):
            return "Sub dir of %s" % str(self.parent)
        else:
            return "File in sub dir of %s" % str(self.parent)

    def _delegate(self, path):
        return pathjoin(self.sub_dir, relpath(normpath(path)))

    def getsyspath(self, path, allow_none=False):
        return self.parent.getsyspath(self._delegate(path), allow_none=allow_none)

    def open(self, path, mode="r", **kwargs):
        return self.parent.open(self._delegate(path), mode)

    def exists(self, path):
        return self.parent.exists(self._delegate(path))

    def opendir(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)

        path = self._delegate(path)
        sub_fs = self.parent.opendir(path)
        return sub_fs

    def isdir(self, path):
        return self.parent.isdir(self._delegate(path))

    def isfile(self, path):
        return self.parent.isfile(self._delegate(path))

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        paths = self.parent.listdir(self._delegate(path),
                                    wildcard=wildcard,
                                    full=False,
                                    absolute=False,
                                    dirs_only=dirs_only,
                                    files_only=files_only)
        if absolute:
            listpath = normpath(path)
            paths = [abspath(pathjoin(listpath, path)) for path in paths]
        elif full:
            listpath = normpath(path)
            paths = [relpath(pathjoin(listpath, path)) for path in paths]
        return paths


    def makedir(self, path, recursive=False, allow_recreate=False):
        return self.parent.makedir(self._delegate(path), recursive=recursive, allow_recreate=allow_recreate)

    def remove(self, path):
        return self.parent.remove(self._delegate(path))

    def removedir(self, path, recursive=False,force=False):
        # Careful not to recurse outside the subdir
        if path in ("","/"):
            if force:
                for path2 in self.listdir(path,absolute=True,files_only=True):
                    try:
                        self.remove(path2)
                    except ResourceNotFoundError:
                        pass
                for path2 in self.listdir(path,absolute=True,dirs_only=True):
                    try:
                        self.removedir(path2,force=True)
                    except ResourceNotFoundError:
                        pass
        else:
            self.parent.removedir(self._delegate(path),force=force)
            if recursive:
                try:
                    self.removedir(dirname(path),recursive=True)
                except DirectoryNotEmptyError:
                    pass

    def settimes(self, path, accessed_time=None, modified_time=None):
        return self.parent.settimes(self._delegate(path), accessed_time, modified_time)

    def getinfo(self, path):
        return self.parent.getinfo(self._delegate(path))

    def getsize(self, path):
        return self.parent.getsize(self._delegate(path))

    def rename(self, src, dst):
        return self.parent.rename(self._delegate(src), self._delegate(dst))

    def move(self, src, dst, **kwds):
        self.parent.move(self._delegate(src),self._delegate(dst),**kwds)

    def movedir(self, src, dst, **kwds):
        self.parent.movedir(self._delegate(src),self._delegate(dst),**kwds)

    def copy(self, src, dst, **kwds):
        self.parent.copy(self._delegate(src),self._delegate(dst),**kwds)

    def copydir(self, src, dst, **kwds):
        self.parent.copydir(self._delegate(src),self._delegate(dst),**kwds)

    def createfile(self, path, data=""):
        return self.parent.createfile(self._delegate(path),data)

    def setcontents(self, path, data=""):
        return self.parent.setcontents(self._delegate(path),data)

    def getcontents(self, path):
        return self.parent.getcontents(self._delegate(path))


def flags_to_mode(flags):
    """Convert an os.O_* flag bitmask into an FS mode string."""    
    if flags & os.O_EXCL:
         raise UnsupportedError("open",msg="O_EXCL is not supported")
    if flags & os.O_WRONLY:
        if flags & os.O_TRUNC:
            mode = "w"
        elif flags & os.O_APPEND:
            mode = "a"
        else:
            mode = "r+"
    elif flags & os.O_RDWR:
        if flags & os.O_TRUNC:
            mode = "w+"
        elif flags & os.O_APPEND:
            mode = "a+"
        else:
            mode = "r+"
    else:
        mode = "r"    
    return mode
