import os
import os.path
import fnmatch
from itertools import chain


class NullFile:

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

    def write(self, str):
        pass

    def writelines(self, *args, **kwargs):
        pass



class FSError(Exception):

    def __init__(self, code, msg, path=None, details=None):

        self.code = code
        self.msg = msg
        self.path = path
        self.details = details

    def __str__(self):

        msg = self.msg % self.__dict__

        return '%s - %s' % (self.code, msg)


class PathError(FSError):

    pass


def _isabsolute(path):
    if path:
        return path[1] in '\\/'
    return False

def normpath(path):
    return path.replace('\\', '/')


def pathjoin(*paths):

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
                raise PathError("INVALID_PATH", "Relative path is invalid: %(path)s", str(paths))
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
    return path.rsplit('/', 1)


def resolvepath(path):
    return pathjoin(path)

def makerelative(path):
    if path.startswith('/'):
        return path[1:]
    return path

def splitpath(path, numsplits=None):

    path = resolvepath(path)
    if numsplits == None:
        return path.split('/')
    else:
        return path.split('/', numsplits)


def print_fs(fs, path="/", max_levels=None, indent=' '*2):

    def print_dir(fs, path, level):

        try:
            dir_listing = [(fs.isdir(p), p) for p in fs.listdir(path)]
        except FSError:
            print indent*level + "... unabled to retrieve directory list (%s) ..." % str(e)
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


class FS(object):



    def open(self, pathname, mode, **kwargs):

        pass

    def open_dir(self, dirname):

        if not self.exists(dirname):
            raise FSError("NO_DIR", "Directory does not exist: %(path)s", dirname)

        sub_fs = SubFS(self, dirname)
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
            paths = [self.abspath(pathjoin(path, p)) for p in paths]


        return paths


class SubFS(FS):

    def __init__(self, parent, sub_dir):

        self.parent = parent
        self.sub_dir = parent.abspath(sub_dir)
        #print "sub_dir", self.sub_dir

    def __str__(self):
        return "<SubFS \"%s\" of %s>" % (self.sub_dir, self.parent)

    def _delegate(self, dirname):

        delegate_path = pathjoin(self.sub_dir, resolvepath(makerelative(dirname)))
        #print "delegate path", delegate_path
        return delegate_path

    def getsyspath(self, pathname):

        return self.parent.getsyspath(self._delegate(pathname))

    def open(self, pathname, mode="r", buffering=-1):

        return self.parent.open(self._delegate(pathname), mode, buffering)

    def isdir(self, pathname):

        return self.parent.isdir(self._delegate(pathname))

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        return self.parent.listdir(self._delegate(path), wildcard, full, absolute, hidden, dirs_only, files_only)


class OSFS(FS):

    def __init__(self, root_path):

        expanded_path = normpath(os.path.expanduser(root_path))
        
        print expanded_path
        
        if not os.path.exists(expanded_path):
            raise FSError("PATH_NOT_EXIST", "Root path does not exist: %(path)s", expanded_path)
        if not os.path.isdir(expanded_path):
            raise FSError("PATH_NOT_DIR", "Root path is not a directory: %(path)s", expanded_path)
        
        self.root_path = normpath(os.path.abspath(expanded_path))
        #print "Root path", self.root_path

    def __str__(self):
        return "<OSFS \"%s\">" % self.root_path

    def _resolve(self, pathname):

        resolved_path = resolvepath(pathname)
        #print "Resolved_path", resolved_path
        return resolved_path

    def getsyspath(self, pathname):

        #print makerelative(self._resolve(pathname))
        sys_path = os.path.join(self.root_path, makerelative(self._resolve(pathname)))
        #print "Sys path", sys_path
        return sys_path

    def abspath(self, pathname):

        pathname = normpath(pathname)

        if not pathname.startswith('/'):
            return pathjoin('/', pathname)
        return pathname


    def open(self, pathname, mode="r", buffering=-1):

        try:
            f = open(self.getsyspath(pathname), mode, buffering)
        except IOError, e:
            raise FSError("OPEN_FAILED", str(e), pathname, details=e)

        return f

    def exists(self, pathname):

        pathname = self.getsyspath(pathname)
        return os.path.exists(pathname)

    def isdir(self, pathname):

        path = self.getsyspath(pathname)
        return os.path.isdir(path)

    def isfile(self, pathname):

        path = self.getsyspath(pathname)
        return os.path.isfile(path)

    def ishidden(self, pathname):

        return pathname.startswith('.')


    def listdir(self, path="./", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        try:
            paths = os.listdir(self.getsyspath(path))
        except IOError, e:
            raise FSError("LIST_FAILED", str(e), path, details=e)

        return self._listdir_helper(path, paths, wildcard, full, absolute, hidden, dirs_only, files_only)





class MountFS(FS):

    TYPE_FILE, TYPE_DIR, TYPE_FS = range(3)

    class DirEntry(object):
        def __init__(self, type, name, contents ):

            assert filename or dirname or fs, "Must specifiy a filename, a dirname or a fs!"

            self.type = type
            self.name = name
            self.contents = contents

        def isdir(self):
            return self.type == TYPE_DIR

        def isfile(self):
            return self.type == TYPE_FILE

        def isfs(self):
            return self.type == TYPE_FS


    def __init__(self):

        self.parent = None

        self.root = {}

    def mkdir(self, dirpath, recursive=True):

        if recursive and '/' in dirpath:
            raise PathError("INVALID_PATH", "Use recursive=True to create this path", dirpath)

        def do_mkdir(dirname):

            if dirname not in current_dir:
                current_dir["dirname"] = {}
                return True
            return False

        current_dir = self.root
        for path_component in split_path(current_dir):

            if path_component in current_dir:
                if not current_dir[path_component].isdir():
                    raise PathError("INVALID_PATH", "Can not create path here", dirpath)

            current_dir[path_component] = DirEntry(TYPE_DIR, path_component, {})
            current_dir = current_dir[path_component].contents

        return self


    def mountdir(self, dirname, dirfs, params=None, create_path=True):

        if dirname in self.dir_mounts:
            raise FSError("MOUNT_NOT_FREE", "A directory of this name is already mounted", dirname)

        success, code = dirfs._onmount(self)
        if success:
            self.dir_mounts[dirname] = dirfs

        return code

    def unmountdir(self, dirname):

        if dirname not in self.dir_mounts:
            raise FSError("NOT_MOUNTED", "Directory not mounted", dirname)

        dirfs = self.dir_mounts[dirname]

        success = dirfs._onunmount(self)
        if success:
            del dirfs[dirname]

        return code



    def mountfile(self, filename, callable, params=None, create_path=True):
        pass

    def unmountfile(self, filename):
        pass



    def _onmount(self, parent):
        pass

    def _onunmount(self, parent):
        pass


    def open(self, pathname, params=None):
        pass


    def opendir(self, dirname, params=None):

        pass

    def isdir(self, dirname):
        pass

    def isfile(self, filename):
        pass

    def listdir(self, pathname, absolute=False):
        pass

    def mkdir(self, pathname):
        pass

    def exists(self, filename):
        pass

    def getsyspath(self, filename):
        return None



if __name__ == "__main__":

    osfs = OSFS("~/")
    print osfs
    #print osfs

    print_fs(osfs)

    #print osfs.listdir("/projects/fs")

    #sub_fs = osfs.open_dir("projects/")

    #print sub_fs

    #sub_fs.open('test.txt')

    #print sub_fs.listdir(dirs_only=True)
    #print sub_fs.listdir()
    #print_fs(sub_fs, max_levels=2)

    #for f in osfs.listdir():
    #    print f

    #print osfs.listdir('projects', dirs_only=True, wildcard="d*")

    #print_fs(osfs, 'projects/')

    print pathjoin('/', 'a')

    print pathjoin('a/b/c', '../../e/f')