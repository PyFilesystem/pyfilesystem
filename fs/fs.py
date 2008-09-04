#!/usr/bin/env python

import os
import os.path
import fnmatch
from itertools import chain
import datetime
try:
    import threading
except ImportError:
    import dummy_threading as threadding

error_msgs = {

    "UNKNOWN_ERROR" :   "No information on error: %(path)s",

    # UnsupportedError
    "UNSUPPORTED" :     "Action is unsupported by this filesystem.",

    # OperationFailedError
    "LISTDIR_FAILED" :      "Unable to get directory listing: %(path)s",
    "DELETE_FAILED" :       "Unable to delete file: %(path)s",
    "RENAME_FAILED" :       "Unable to rename file: %(path)s",
    "OPEN_FAILED" :         "Unable to open file: %(path)s",
    "DIR_EXISTS" :          "Directory exists (try allow_recreate=True): %(path)s",
    "REMOVE_FAILED" :       "Unable to remove file: %(path)s",
    "REMOVEDIR_FAILED" :    "Unable to remove dir: %(path)s",

    # NoSysPathError
    "NO_SYS_PATH" :     "No mapping to OS filesytem: %(path)s,",

    # PathError
    "INVALID_PATH" :    "Path is invalid: %(path)s",

    # ResourceLockedError
    "FILE_LOCKED" :     "File is locked: %(path)s",
    "DIR_LOCKED" :      "Dir is locked: %(path)s",

    # ResourceNotFoundError
    "NO_DIR" :          "Directory does not exist: %(path)s",
    "NO_FILE" :         "No such file: %(path)s",
    "NO_RESOURCE" :     "No path to: %(path)s",

    # ResourceInvalid
    "WRONG_TYPE" :      "Resource is not the type that was expected: %(path)s",

    # SystemError
    "OS_ERROR" :        "Non specific OS error: %(path)s",
}

error_codes = error_msgs.keys()

class FSError(Exception):

    """A catch all exception for FS objects."""

    def __init__(self, code, path=None, msg=None, details=None):
        """

        code -- A short identifier for the error
        path -- A path associated with the error
        msg -- An textual description of the error
        details -- Any additional details associated with the error

        """

        self.code = code
        self.msg = msg or error_msgs.get(code, error_msgs['UNKNOWN_ERROR'])
        self.path = path
        self.details = details

    def __str__(self):
        msg = self.msg % dict((k, str(v)) for k, v in self.__dict__.iteritems())

        return '%s. %s' % (self.code, msg)

class UnsupportedError(FSError): pass
class OperationFailedError(FSError): pass
class NoSysPathError(FSError): pass
class PathError(FSError): pass
class ResourceLockedError(FSError): pass
class ResourceNotFoundError(FSError): pass
class SystemError(FSError): pass
class ResourceInvalid(FSError): pass


class NullFile(object):

    """A NullFile is a file object that has no functionality. Null files are
    returned by the 'safeopen' method in FS objects when the file does not exist.
    This can simplify code by negating the need to check if a file exists,
    or handling exceptions.

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


def isabsolutepath(path):
    """Returns True if a given path is absolute.

    >>> isabsolutepath("a/b/c")
    False

    >>> isabsolutepath("/foo/bar")
    True

    """
    if path:
        return path[0] in '\\/'
    return False

def normpath(path):
    """Normalizes a path to be in the formated expected by FS objects.
    Returns a new path string.

    >>> normpath(r"foo\\bar\\baz")
    'foo/bar/baz'

    """
    return path.replace('\\', '/')


def pathjoin(*paths):
    """Joins any number of paths together. Returns a new path string.

    paths -- An iterable of path strings

    >>> pathjoin('foo', 'bar', 'baz')
    'foo/bar/baz'

    >>> pathjoin('foo/bar', '../baz')
    'foo/baz'

    """
    absolute = False

    relpaths = []
    for p in paths:
        if p:
         if p[0] in '\\/':
             del relpaths[:]
             absolute = True
         relpaths.append(p)

    pathstack = []

    for component in chain(*(normpath(path).split('/') for path in relpaths)):
        if component == "..":
            if not pathstack:
                raise PathError("INVALID_PATH", repr(paths), msg="relative path is invalid")
            sub = pathstack.pop()
        elif component == ".":
            pass
        elif component:
            pathstack.append(component)

    if absolute:
        return "/" + "/".join(pathstack)
    else:
        return "/".join(pathstack)


def pathsplit(path):
    """Splits a path on a path separator. Returns a tuple containing the path up
    to that last separator and the remaining path component.

    >>> pathsplit("foo/bar")
    ('foo', 'bar')

    >>> pathsplit("foo/bar/baz")
    ('foo/bar', 'baz')

    """

    split = normpath(path).rsplit('/', 1)
    if len(split) == 1:
        return ('', split[0])
    return tuple(split)

def resolvepath(path):
    """Normalises the path and removes any relative path components.

    path -- A path string

    >>> resolvepath(r"foo\\bar\\..\\baz")
    'foo/baz'

    """
    return pathjoin(path)

def makerelative(path):
    """Makes a path relative by removing initial separator.

    path -- A path

    >>> makerelative("/foo/bar")
    'foo/bar'

    """
    path = normpath(path)
    if path.startswith('/'):
        return path[1:]
    return path

def makeabsolute(path):
    """Makes a path absolute by adding a separater at the beginning of the path.

    path -- A path

    >>> makeabsolute("foo/bar/baz")
    '/foo/bar/baz'

    """
    path = normpath(path)
    if not path.startswith('/'):
        return '/'+path
    return path

def _iteratepath(path, numsplits=None):

    path = resolvepath(path)
    if not path:
        return []

    if numsplits == None:
        return filter(lambda p:bool(p), path.split('/'))
    else:
        return filter(lambda p:bool(p), path.split('/', numsplits))


def print_fs(fs, path="/", max_levels=None, indent=' '*2):
    """Prints a filesystem listing to stdout (including sub dirs). Useful as a debugging aid.
    Be careful about printing a OSFS, or any other large filesystem.
    Without max_levels set, this function will traverse the entire directory tree.

    fs -- A filesystem object
    path -- Path of root to list (default "/")
    max_levels -- Maximum levels of dirs to list (default None for no maximum)
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
            else:
                print indent*level + '%s' % item
    print_dir(fs, path, 0)


def _synchronize(func):
    def acquire_lock(self, *args, **kwargs):
        self._lock.acquire()
        try:
            return func(self, *args, **kwargs)
        finally:
            self._lock.release()
    acquire_lock.__doc__ = func.__doc__
    return acquire_lock



class FS(object):

    """The base class for Filesystem objects. An instance of a class derived from FS is an abstraction
    on some kind of filesytem, such as the OS filesystem or a zip file.

    """

    def __init__(self, thread_syncronize=False):

        if thread_syncronize:
            self._lock = threading.RLock()
        else:
            self._lock = None

    def _resolve(self, pathname):
        resolved_path = resolvepath(pathname)
        return resolved_path

    def _abspath(self, pathname):
        pathname = normpath(pathname)

        if not pathname.startswith('/'):
            return pathjoin('/', pathname)
        return pathname

    def getsyspath(self, path, default=None):
        """Returns the system path (a path recognised by the operating system) if present.
        If the path does not map to a system path, then either the default is returned (if given),
        or a NoSysPathError exception is thrown.

        path -- A path within the filesystem
        default -- A default value to return if there is no mapping to an operating system path

        """
        if default is None:
            raise NoSysPathError("NO_SYS_PATH", path)
        return default

    def open(self, path, mode="r", buffering=-1, **kwargs):
        raise UnsupportedError("UNSUPPORTED")

    def safeopen(self, *args, **kwargs):
        try:
            f = self.open(*args, **kwargs)
        except ResourceNotFoundError:
            return NullFile()
        return f

    def exists(self, path):
        raise UnsupportedError("UNSUPPORTED")

    def isdir(self, path):
        raise UnsupportedError("UNSUPPORTED")

    def isfile(self, path):
        raise UnsupportedError("UNSUPPORTED")

    def ishidden(self, path):
        return path.startswith('.')

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):
        raise UnsupportedError("UNSUPPORTED")

    def makedir(self, path, mode=0777, recursive=False):
        raise UnsupportedError("UNSUPPORTED")

    def remove(self, path):
        raise UnsupportedError("UNSUPPORTED")

    def removedir(self, path, recursive=False):
        raise UnsupportedError("UNSUPPORTED")

    def rename(self, src, dst):
        raise UnsupportedError("UNSUPPORTED")

    def getinfo(self, path):
        raise UnsupportedError("UNSUPPORTED")

    def desc(self, path):
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

    def open(self, path, mode="r", buffering=-1, **kwargs):
        raise UNSUPPORTED_ERROR("UNSUPPORTED")

    def opendir(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError("NO_DIR", path)

        sub_fs = SubFS(self, path)
        return sub_fs

    def _listdir_helper(self, path, paths, wildcard, full, absolute, hidden, dirs_only, files_only):
        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only can not both be True")

        if wildcard is not None:
            match = fnmatch.fnmatch
            paths = [p for p in path if match(p, wildcard)]

        if not hidden:
            paths = [p for p in paths if not self.ishidden(p)]

        if dirs_only:
            paths = [p for p in paths if self.isdir(pathjoin(path, p))]
        elif files_only:
            paths = [p for p in paths if self.isfile(pathjoin(path, p))]

        if full:
            paths = [pathjoin(path, p) for p in paths]
        elif absolute:
            paths = [self._abspath(pathjoin(path, p)) for p in paths]

        return paths


    def walkfiles(self, path="/", wildcard=None, dir_wildcard=None):
        dirs = [path]
        files = []

        while dirs:

            current_path = dirs.pop()

            for path in self.listdir(current_path, full=True):
                if self.isdir(path):
                    if dir_wildcard is not None:
                        if fnmatch.fnmatch(path, dir_wilcard):
                            dirs.append(path)
                    else:
                        dirs.append(path)
                else:
                    if wildcard is not None:
                        if fnmatch.fnmatch(path, wildcard):
                            yield path
                    else:
                        yield path

    def walk(self, path="/", wildcard=None, dir_wildcard=None):
        dirs = [path]
        while dirs:
            current_path = dirs.pop()

            paths = []
            for path in self.listdir(current_path, full=True):

                if self.isdir(path):
                    if dir_wildcard is not None:
                        if fnmatch.fnmatch(path, dir_wilcard):
                            dirs.append(path)
                    else:
                        dirs.append(path)
                else:
                    if wildcard is not None:
                        if fnmatch.fnmatch(path, wildcard):
                            paths.append(path)
                    else:
                        paths.append(path)
            yield (current_path, paths)


    def getsize(self, path):
        return self.getinfo(path)['size']



class SubFS(FS):

    """A SubFS represents a sub directory of another filesystem object.
    SubFS objects are return by opendir, which effectively creates a 'sandbox'
    filesystem that can only access files / dirs under a root path within its 'parent' dir.

    """

    def __init__(self, parent, sub_dir):
        self.parent = parent
        self.sub_dir = parent._abspath(sub_dir)

    def __str__(self):
        return "<SubFS \"%s\" in %s>" % (self.sub_dir, self.parent)

    def desc(self, path):
        if self.isdir(path):
            return "Sub dir of %s"%str(self.parent)
        else:
            return "File in sub dir of %s"%str(self.parent)

    def _delegate(self, path):
        return pathjoin(self.sub_dir, resolvepath(makerelative(path)))

    def getsyspath(self, path):
        return self.parent.getsyspath(self._delegate(path))

    def open(self, path, mode="r", buffering=-1, **kwargs):
        return self.parent.open(self._delegate(path), mode, buffering)

    def exists(self, path):
        return self.parent.exists(self._delegate(path))

    def opendir(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError("NO_DIR", path)

        path = self._delegate(path)
        sub_fs = self.parent.opendir(path)
        return sub_fs

    def isdir(self, path):
        return self.parent.isdir(self._delegate(path))

    def isfile(self, path):
        return self.parent.isdir(self._delegate(path))

    def ishidden(self, path):
        return self.parent.ishidden(self._delegate(path))

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):
        paths = self.parent.listdir(self._delegate(path),
                                    wildcard,
                                    False,
                                    False,
                                    hidden,
                                    dirs_only,
                                    files_only)
        if absolute:
            listpath = resolvepath(path)
            paths = [makeabsolute(pathjoin(listpath, path)) for path in paths]
        elif full:
            listpath = resolvepath(path)
            paths = [makerelative(pathjoin(listpath, path)) for path in paths]
        return paths


    def makedir(self, path, mode=0777, recursive=False):
        return self.parent.makedir(self._delegate(path), mode=mode, recursive=recursive)

    def remove(self, path):
        return self.parent.remove(self._delegate(path))

    def removedir(self, path, recursive=False):
        self.parent.removedir(self._delegate(path), recursive=recursive)

    def getinfo(self, path):
        return self.parent.getinfo(self._delegate(path))

    def getsize(self, path):
        return self.parent.getsize(self._delegate(path))

    def rename(self, src, dst):
        return self.parent.rename(self._delegate(src), self._delegate(dst))

def validatefs(fs):

    expected_methods = [ "abspath",
                         "getsyspath",
                         "open",
                         "exists",
                         "isdir",
                         "isfile",
                         "ishidden",
                         "listdir",
                         "makedir",
                         "remove",
                         "removedir",
                         "getinfo",
                         "getsize",
                         "rename",
    ]

    pad_size = len(max(expected_methods, key=str.__len__))
    count = 0
    for method_name in sorted(expected_methods):
        method = getattr(fs, method_name, None)
        if method is None:
            print method_name.ljust(pad_size), '?'
        else:
            print method_name.ljust(pad_size), 'X'
            count += 1
    print
    print "%i out of %i methods" % (count, len(expected_methods))


if __name__ == "__main__":
    import osfs
    import browsewin

    fs1 = osfs.OSFS('~/')
    fs2 = fs1.opendir("projects").opendir('prettycharts')

    #browsewin.browse(fs1)
    browsewin.browse(fs2)