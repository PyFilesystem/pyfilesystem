#!/usr/bin/env python
"""

  fs.base:  base class defining the FS abstraction.

This module defines the most basic filesystem abstraction, the FS class.
Instances of FS represent a filesystem containing files and directories
that can be queried and manipulated.  To implement a new kind of filesystem,
start by sublcassing the base FS class.

"""

import shutil
import fnmatch
import datetime
try:
    import threading
except ImportError:
    import dummy_threading as threading
import dummy_threading
try:
    import cPickle as pickle
except ImportError:
    import pickle


from helpers import *
from errors import *


def silence_fserrors(f, *args, **kwargs):
    """Perform a function call and return None if FSError is thrown

    f -- Function to call
    args -- Parameters to f
    kwargs -- Keyword parameters to f

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

    def close(self):
        self.closed = True

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


def print_fs(fs, path="/", max_levels=5, indent=' '*2):
    """Prints a filesystem listing to stdout (including sub dirs).

    Useful as a debugging aid. Be careful about printing a OSFS, or any other
    large filesystem - without max_levels set, this function will traverse the 
    entire directory tree.

    fs -- A filesystem object
    path -- Path of root to list (default "/")
    max_levels -- Maximum levels of dirs to list (default 5), set to None
                  for no maximum
    indent -- String to indent each directory level (default two spaces)

    """
    def print_dir(fs, path, level):
        try:
            dir_listing = [(fs.isdir(pathjoin(path,p)), p) for p in fs.listdir(path)]
        except FSError, e:
            print indent*level + "... unabled to retrieve directory list (reason: %s) ..." % str(e)
            return

        dir_listing.sort(key = lambda (isdir, p):(not isdir, p.lower()))

        for is_dir, item in dir_listing:

            if is_dir:
                print indent*level + '[%s]' % item
                if max_levels is None or level < max_levels:
                    print_dir(fs, pathjoin(path, item), level+1)
                if max_levels is not None:
                    if level >= max_levels:
                        print indent*(level+1) + "..."
            else:
                print indent*level + '%s' % item
    print_dir(fs, path, 0)


def _synchronize(func):
    """Decorator to synchronize a method on self._lock."""
    def acquire_lock(self, *args, **kwargs):
        self._lock.acquire()
        try:
            return func(self, *args, **kwargs)
        finally:
            self._lock.release()
    acquire_lock.__doc__ = func.__doc__
    return acquire_lock


class FS(object):
    """The base class for Filesystem abstraction objects.

    An instance of a class derived from FS is an abstraction on some kind
    of filesytem, such as the OS filesystem or a zip file.

    The following is the minimal set of methods that must be provided by
    a new FS subclass:

        * open -- open a file for reading/writing (like python's open() func)
        * isfile -- check whether a path exists and is a file
        * isdir -- check whether a path exists and is a directory
        * listdir -- list the contents of a directory
        * makedir -- create a new directory
        * remove -- remove an existing file
        * removedir -- remove an existing directory
        * rename -- atomically rename a file or directory
        * getinfo -- return information about the path e.g. size, mtime

    The following methods have a sensible default implementation, but FS
    subclasses are welcome to override them if a more efficient implementation
    can be provided:

        * getsyspath -- get a file's name in the local filesystem, if possible 
        * exists -- check whether a path exists as file or directory
        * copy -- copy a file to a new location
        * move -- move a file to a new location
        * copydir -- recursively copy a directory to a new location
        * movedir -- recursively move a directory to a new location

    """

    def __init__(self, thread_synchronize=False):
        """The base class for Filesystem objects.

        thread_synconize -- If True, a lock object will be created for the
        object, otherwise a dummy lock will be used.

        """
        if thread_synchronize:
            self._lock = threading.RLock()
        else:
            self._lock = dummy_threading.RLock()


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
                self._lock = dummy_threading.RLock()


    def getsyspath(self, path, allow_none=False):
        """Returns the system path (a path recognised by the OS) if present.

        If the path does not map to a system path (and allow_none is False)
        then a NoSysPathError exception is thrown.

        path -- A path within the filesystem
        allow_none -- If True, this method should return None if there is no
                      system path, rather than raising NoSysPathError

        """
        if not allow_none:
            raise NoSysPathError(path=path)
        return None

    def hassyspath(self, path):
        """Return True if the path maps to a system path.

        path -- Pach to check

        """
        return self.getsyspath(path, None) is not None


    def open(self, path, mode="r", **kwargs):
        """Open a the given path as a file-like object.

        path -- Path to file that should be opened
        mode -- Mode of file to open, identical to the mode string used
               in 'file' and 'open' builtins
        kwargs -- Additional (optional) keyword parameters that may
                  be required to open the file

        """
        raise UnsupportedError("open file")

    def safeopen(self, *args, **kwargs):
        """Like 'open', but returns a NullFile if the file could't be opened."""
        try:
            f = self.open(*args, **kwargs)
        except ResourceNotFoundError:
            return NullFile()
        return f


    def exists(self, path):
        """Returns True if the path references a valid resource."""
        return self.isfile(path) or self.isdir(path)

    def isdir(self, path):
        """Returns True if a given path references a directory."""
        raise UnsupportedError("check for directory")

    def isfile(self, path):
        """Returns True if a given path references a file."""
        raise UnsupportedError("check for file")


    def listdir(self,   path="./",
                        wildcard=None,
                        full=False,
                        absolute=False,
                        dirs_only=False,
                        files_only=False):
        """Lists all the files and directories in a path.

        path -- Root of the path to list
        wildcard -- Only returns paths that match this wildcard
        full -- Returns a full path
        absolute -- Returns an absolute path
        dirs_only -- If True, only return directories
        files_only -- If True, only return files

        The directory contents are returned as a list of paths.  If the
        given path is not found then ResourceNotFoundError is raised;
        if it exists but is not a directory, ResourceInvalidError is raised.

        """
        raise UnsupportedError("list directory")

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
            match = fnmatch.fnmatch
            entries = [p for p in entries if match(p, wildcard)]

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

        path -- Path of directory
        recursive -- If True, also create intermediate directories
        allow_recreate -- If True, re-creating a directory wont be an error

        The following errors can be raised by this method:
          * DestinationExistsError, if path is already a directory and
                                    allow_recreate is False
          * ParentDirectoryMissingError, if a containing directory is missing
                                         and recursive is False
          * ResourceInvalidError, if path is an existing file

        """
        raise UnsupportedError("make directory")

    def remove(self, path):
        """Remove a file from the filesystem.

        path -- Path of the resource to remove

        This method can raise the following errors:
          * ResourceNotFoundError, if the path does not exist
          * ResourceInvalidError, if the path is a directory

        """
        raise UnsupportedError("remove resource")

    def removedir(self, path, recursive=False, force=False):
        """Remove a directory from the filesystem

        path -- Path of the directory to remove
        recursive -- If True, then empty parent directories will be removed
        force -- If True, any directory contents will be removed

        This method can raise the following errors:
          * ResourceNotFoundError, if the path does not exist
          * ResourceInvalidError, if the path is not a directory
          * DirectoryNotEmptyError, if the directory is not empty and
                                   force is False

        """
        raise UnsupportedError("remove directory")

    def rename(self, src, dst):
        """Renames a file or directory

        src -- Path to rename
        dst -- New name (not a path)

        """
        raise UnsupportedError("rename resource")

    def getinfo(self, path):
        """Returns information for a path as a dictionary.

        path -- A path to retrieve information for

        """
        raise UnsupportedError("get resource info")


    def desc(self, path):
        """Returns short descriptive text regarding a path.

        path -- A path to describe

        This is mainly for use as a debugging aid.

        """
        if not self.exists(path):
            return "No description available"
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

        path -- path of file to read.

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

        path -- Path of the file to create
        data -- A string containing the contents of the file

        """
        f = None
        try:
            f = self.open(path, 'wb')
            f.write(data)
        finally:
            if f is not None:
                f.close()
    setcontents = createfile

    def opendir(self, path):
        """Opens a directory and returns a FS object representing its contents.

        path -- Path to directory to open

        """
        if not self.exists(path):
            raise DirectoryNotFoundError(path)
        sub_fs = SubFS(self, path)
        return sub_fs


    def walk(self, path="/", wildcard=None, dir_wildcard=None, search="breadth"):
        """Walks a directory tree and yields the root path and contents.
        Yields a tuple of the path of each directory and a list of its file contents.

        path -- Root path to start walking
        wildcard -- If given, only return files that match this wildcard
        dir_wildcard -- If given, only walk in to directories that match this wildcard
        search -- A string that identifies the method used to walk the directories,
        can be 'breadth' for a breadth first search, or 'depth' for a depth first
        search. Use 'depth' if you plan to create / delete files as you go.


        """
        if search == "breadth":
            dirs = [path]
            while dirs:
                current_path = dirs.pop()

                paths = []
                for filename in self.listdir(current_path):

                    path = pathjoin(current_path, filename)
                    if self.isdir(path):
                        if dir_wildcard is not None:
                            if fnmatch.fnmatch(path, dir_wilcard):
                                dirs.append(path)
                        else:
                            dirs.append(path)
                    else:
                        if wildcard is not None:
                            if fnmatch.fnmatch(path, wildcard):
                                paths.append(filename)
                        else:
                            paths.append(filename)
                yield (current_path, paths)

        elif search == "depth":

            def recurse(recurse_path):
                for path in self.listdir(recurse_path, wildcard=dir_wildcard, full=True, dirs_only=True):
                    for p in recurse(path):
                        yield p
                yield (recurse_path, self.listdir(recurse_path, wildcard=wildcard, files_only=True))

            for p in recurse(path):
                yield p
        else:
            raise ValueError("Search should be 'breadth' or 'depth'")


    def walkfiles(self, path="/", wildcard=None, dir_wildcard=None, search="breadth" ):
        """Like the 'walk' method, but just yields files.

        path -- Root path to start walking
        wildcard -- If given, only return files that match this wildcard
        dir_wildcard -- If given, only walk in to directories that match this wildcard
        search -- A string that identifies the method used to walk the directories,
        can be 'breadth' for a breadth first search, or 'depth' for a depth first
        search. Use 'depth' if you plan to create / delete files as you go.

        """

        for path, files in self.walk(path, wildcard, dir_wildcard, search):
            for f in files:
                yield pathjoin(path, f)


    def getsize(self, path):
        """Returns the size (in bytes) of a resource.

        path -- A path to the resource

        """
        info = self.getinfo(path)
        size = info.get('size', None)
        if 'size' is None:
            raise OperationFailedError("get size of resource", path)
        return size

    def copy(self, src, dst, overwrite=False, chunk_size=16384):
        """Copies a file from src to dst.

        src -- The source path
        dst -- The destination path
        overwrite -- If True, then an existing file at the destination may
                     be overwritten; If False then DestinationExistsError
                     will be raised.
        chunk_size -- Size of chunks to use if a simple copy is required

        If the destination path exists and is a directory, the file will
        be copied into that directory.
        """

        if self.isdir(dst):
            dst = pathjoin( dirname(dst), resourcename(src) )

        if not self.isfile(src):
            raise ResourceInvalidError(src,msg="Source is not a file: %(path)s")
        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)

        src_syspath = self.getsyspath(src, allow_none=True)
        dst_syspath = self.getsyspath(dst, allow_none=True)

        if src_syspath is not None and dst_syspath is not None:
            shutil.copyfile(src_syspath, dst_syspath)
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


    def move(self, src, dst, overwrite=False, chunk_size=16384):
        """Moves a file from one location to another.

        src -- Source path
        dst -- Destination path
        overwrite -- If True, then an existing file at the destination path
                     will be silently overwritte; if False then an exception
                     will be raised in this case.

        """

        src_syspath = self.getsyspath(src, allow_none=True)
        dst_syspath = self.getsyspath(dst, allow_none=True)

        if src_syspath is not None and dst_syspath is not None:
            if not self.isfile(src):
                raise ResourceInvalidError(src, msg="Source is not a file: %(path)s")
            if not overwrite and self.exists(dst):
                raise DestinationExistsError(dst)
            shutil.move(src_syspath, dst_syspath)
        else:
            self.copy(src, dst, overwrite=overwrite, chunk_size=chunk_size)
            self.remove(src)


#    def _get_attr_path(self, path):
#        if self.isdir(path):
#            return pathjoin(path, '.xattrs.')
#        else:
#            dir_path, file_path = pathsplit(path)
#            return pathjoin(dir_path, '.xattrs.'+file_path)
#
#    def _get_attr_dict(self, path):
#        attr_path = self._get_attr_path(path)
#        if self.exists(attr_path):
#            return pickle.loads(self.getcontents(attr_path))
#        else:
#            return {}
#
#    def _set_attr_dict(self, path, attrs):
#        attr_path = self._get_attr_path(path)
#        self.setcontents(self._get_attr_path(path), pickle.dumps(attrs))
#
#    def setxattr(self, path, key, value):
#        attrs = self._get_attr_dict(path)
#        attrs[key] = value
#        self._set_attr_dict(path, attrs)
#
#    def getxattr(self, path, key, default):
#        attrs = self._get_attr_dict(path)
#        return attrs.get(key, default)
#
#    def delxattr(self, path, key):
#        attrs = self._get_attr_dict(path)
#        try:
#            del attrs[key]
#        except KeyError:
#            pass
#        self._set_attr_dict(path, attrs)
#
#    def listxattrs(self, path):
#        attrs = self._get_attr_dict(path)
#        return self._get_attr_dict(path).keys()
#
#    def getxattrs(self, path):
#        return dict( [(k, self.getxattr(path, k)) for k in self.listxattrs(path)] )

    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        """Moves a directory from one location to another.

        src -- Source directory path
        dst -- Destination directory path
        overwrite -- If True then any existing files in the destination directory will be overwritten
        ignore_errors -- If True then this method will ignore FSError exceptions when moving files
        chunk_size -- Size of chunks to use when copying, if a simple copy is required

        """
        if not self.isdir(src):
            raise ResourceInvalidError(src, msg="Source is not a directory: %(path)s")
        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)

        src_syspath = self.getsyspath(src, allow_none=True)
        dst_syspath = self.getsyspath(dst, allow_none=True)

        if src_syspath is not None and dst_syspath is not None:
            try:
                shutil.move(src_syspath, dst_syspath)
                return
            except WindowsError:
                pass

        def movefile_noerrors(src, dst, overwrite):
            try:
                return self.move(src, dst, overwrite)
            except FSError:
                return
        if ignore_errors:
            movefile = movefile_noerrors
        else:
            movefile = self.move

        self.makedir(dst, allow_recreate=True)
        for dirname, filenames in self.walk(src, search="depth"):

            dst_dirname = makerelative(dirname[len(src):])
            dst_dirpath = pathjoin(dst, dst_dirname)
            self.makedir(dst_dirpath, allow_recreate=True, recursive=True)

            for filename in filenames:

                src_filename = pathjoin(dirname, filename)
                dst_filename = pathjoin(dst_dirpath, filename)
                movefile(src_filename, dst_filename, overwrite=overwrite, chunk_size=chunk_size)

#            attr_path = self._get_attr_path(dirname)
#            if self.exists(attr_path):
#                 self.move(attr_path,self._get_attr_path(dst_dirname),overwrite=True)
#
#            self.removedir(dirname)



    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        """Copies a directory from one location to another.

        src -- Source directory path
        dst -- Destination directory path
        overwrite -- If True then any existing files in the destination directory will be overwritten
        ignore_errors -- If True, exceptions when copying will be ignored
        chunk_size -- Size of chunks to use when copying, if a simple copy is required

        """
        if not self.isdir(src):
            raise ResourceInvalidError(src, msg="Source is not a directory: %(path)s")
        if not overwrite and self.exists(dst):
            raise DestinationExistsError(dst)

        def copyfile_noerrors(src, dst, overwrite):
            try:
                return self.copy(src, dst, overwrite=overwrite)
            except FSError:
                return
        if ignore_errors:
            copyfile = copyfile_noerrors
        else:
            copyfile = self.copy

        copyfile = self.copy
        self.makedir(dst, allow_recreate=True)
        for dirname, filenames in self.walk(src):

            dst_dirname = makerelative(dirname[len(src):])
            dst_dirpath = pathjoin(dst, dst_dirname)
            self.makedir(dst_dirpath, allow_recreate=True)

            for filename in filenames:

                src_filename = pathjoin(dirname, filename)
                dst_filename = pathjoin(dst_dirpath, filename)
                copyfile(src_filename, dst_filename, overwrite=overwrite, chunk_size=chunk_size)

#            attr_path = self._get_attr_path(dirname)
#            if self.exists(attr_path):
#                 self.copy(attr_path,self._get_attr_path(dst_dirname),overwrite=True)


    def isdirempty(self, path):
        """Return True if a path contains no files.

        path -- Path of a directory

        """
        path = normpath(path)
        iter_dir = iter(self.listdir(path))
        try:
            iter_dir.next()
        except StopIteration:
            return True
        return False



class SubFS(FS):
    """A SubFS represents a sub directory of another filesystem object.

    SubFS objects are returned by opendir, which effectively creates a 'sandbox'
    filesystem that can only access files/dirs under a root path within its
    'parent' dir.

    """

    def __init__(self, parent, sub_dir):
        self.parent = parent
        self.sub_dir = parent._abspath(sub_dir)

    def __str__(self):
        return "<SubFS: %s in %s>" % (self.sub_dir, self.parent)

    def __repr__(self):
        return str(self)

    def __unicode__(self):
        return unicode(self.__str__())

    def desc(self, path):
        if self.isdir(path):
            return "Sub dir of %s"%str(self.parent)
        else:
            return "File in sub dir of %s"%str(self.parent)

    def _delegate(self, path):
        return pathjoin(self.sub_dir, relpath(path))

    def getsyspath(self, path, allow_none=False):
        return self.parent.getsyspath(self._delegate(path), allow_none=allow_none)

    def open(self, path, mode="r", **kwargs):
        return self.parent.open(self._delegate(path), mode)

    def exists(self, path):
        return self.parent.exists(self._delegate(path))

    def opendir(self, path):
        if not self.exists(path):
            raise DirectoryNotFoundError(path)

        path = self._delegate(path)
        sub_fs = self.parent.opendir(path)
        return sub_fs

    def isdir(self, path):
        return self.parent.isdir(self._delegate(path))

    def isfile(self, path):
        return self.parent.isfile(self._delegate(path))

    def ishidden(self, path):
        return self.parent.ishidden(self._delegate(path))

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        paths = self.parent.listdir(self._delegate(path),
                                    wildcard,
                                    False,
                                    False,
                                    dirs_only,
                                    files_only)
        if absolute:
            listpath = normpath(path)
            paths = [makeabsolute(pathjoin(listpath, path)) for path in paths]
        elif full:
            listpath = normpath(path)
            paths = [makerelative(pathjoin(listpath, path)) for path in paths]
        return paths


    def makedir(self, path, recursive=False, allow_recreate=False):
        return self.parent.makedir(self._delegate(path), mode=mode, recursive=recursive, allow_recreate=allow_recreate)

    def remove(self, path):
        return self.parent.remove(self._delegate(path))

    def removedir(self, path, recursive=False,force=False):
        self.parent.removedir(self._delegate(path), recursive=recursive, force=force)

    def getinfo(self, path):
        return self.parent.getinfo(self._delegate(path))

    def getsize(self, path):
        return self.parent.getsize(self._delegate(path))

    def rename(self, src, dst):
        return self.parent.rename(self._delegate(src), self._delegate(dst))



if __name__ == "__main__":
    import osfs
    import browsewin

    fs1 = osfs.OSFS('~/')
    fs2 = fs1.opendir("projects").opendir('prettycharts')

    for d, f in fs1.walk('/projects/prettycharts'):
        print d, f

    for f in fs1.walkfiles("/projects/prettycharts"):
        print f

    #print_fs(fs2)


    #browsewin.browse(fs1)
    browsewin.browse(fs2)
