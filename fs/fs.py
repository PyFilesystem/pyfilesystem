import os
import os.path
import fnmatch
from itertools import chain


error_msgs = {

    "UNKNOWN_ERROR" :   "No information on error: %(path)s",
    "INVALID_PATH" :    "Path is invalid: %(path)s",
    "NO_DIR" :          "Directory does not exist: %(path)s",
    "NO_FILE" :         "No such file: %(path)s",
    "LISTDIR_FAILED" :  "Unable to get directory listing: %(path)s",
    "NO_SYS_PATH" :     "No mapping to OS filesytem: %(path)s,",
    "DIR_EXISTS" :      "Directory exists (try allow_recreate=True): %(path)s",
    "OPEN_FAILED" :     "Unable to open file: %(path)s"
}

error_codes = error_msgs.keys()

class FSError(Exception):

    def __init__(self, code, path=None, msg=None, details=None):

        self.code = code
        self.msg = msg or error_msgs.get(code, error_msgs['UNKNOWN_ERROR'])
        self.path = path
        self.details = details

    def __str__(self):

        msg = self.msg % dict((k, str(v)) for k,v in self.__dict__.iteritems())

        return '%s %s' % (self.code, msg)


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

    def write(self, data):
        pass

    def writelines(self, *args, **kwargs):
        pass





def isabsolutepath(path):
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
                raise PathError("INVALID_PATH", str(paths))
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
    split = normpath(path).rsplit('/', 1)
    if len(split) == 1:
        return ('', split[0])
    return split


def resolvepath(path):
    return pathjoin(path)

def makerelative(path):
    if path.startswith('/'):
        return path[1:]
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

    def print_dir(fs, path, level):

        try:
            dir_listing = [(fs.isdir(pathjoin(path,p)), p) for p in fs.listdir(path)]
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


    def _resolve(self, pathname):

        resolved_path = resolvepath(pathname)        
        return resolved_path


    def abspath(self, pathname):

        pathname = normpath(pathname)

        if not pathname.startswith('/'):
            return pathjoin('/', pathname)
        return pathname


    def open(self, pathname, mode="r", buffering=-1, **kwargs):

        pass

    def open_dir(self, dirname):

        if not self.exists(dirname):
            raise FSError("NO_DIR", dirname)

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

    def open(self, pathname, mode="r", buffering=-1, **kwargs):

        return self.parent.open(self._delegate(pathname), mode, buffering)

    def isdir(self, pathname):

        return self.parent.isdir(self._delegate(pathname))

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        return self.parent.listdir(self._delegate(path), wildcard, full, absolute, hidden, dirs_only, files_only)


class OSFS(FS):

    def __init__(self, root_path):

        expanded_path = normpath(os.path.expanduser(root_path))
        
        if not os.path.exists(expanded_path):
            raise FSError("NO_DIR", expanded_path, msg="Root directory does not exist: %(path)s")
        if not os.path.isdir(expanded_path):
            raise FSError("NO_DIR", expanded_path, msg="Root path is not a directory: %(path)s")
        
        self.root_path = normpath(os.path.abspath(expanded_path))
        #print "Root path", self.root_path

    def __str__(self):
        return "<OSFS \"%s\">" % self.root_path



    def getsyspath(self, pathname):
        
        sys_path = os.path.join(self.root_path, makerelative(self._resolve(pathname)))        
        return sys_path



    def open(self, pathname, mode="r", buffering=-1, **kwargs):

        try:
            f = open(self.getsyspath(pathname), mode, buffering)
        except IOError, e:
            raise FSError("OPEN_FAILED", pathname, details=e, msg=str(details))

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
            raise FSError("LISTDIR_FAILED", path, details=e, msg="Unable to get directory listing: %(path)s - (%(details)s)")

        return self._listdir_helper(path, paths, wildcard, full, absolute, hidden, dirs_only, files_only)


    def mkdir(self, path, mode=0777, recursive=False):
        
        sys_path = self.getsyspath(path)
        
        if recursive:
            makedirs(sys_path, mode)
        else:
            makedir(sys_path, mode)
            
            
    def remove(self, path):
        
        sys_path = self.getsyspath(path)
        os.remove(sys_path)





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
            raise PathError("NO_DIR", dirpath, msg="Use recursive=True to create this path: %(path)s")

        def do_mkdir(dirname):

            if dirname not in current_dir:
                current_dir["dirname"] = {}
                return True
            return False

        current_dir = self.root
        for path_component in split_path(current_dir):

            if path_component in current_dir:
                if not current_dir[path_component].isdir():
                    raise PathError("NO_DIR", dirpath, msg="Path references a file, not a dir: %(path)s")

            current_dir[path_component] = DirEntry(TYPE_DIR, path_component, {})
            current_dir = current_dir[path_component].contents

        return self


    def mountdir(self, dirname, dirfs, params=None, create_path=True):

        if dirname in self.dir_mounts:
            raise FSError("MOUNT_NOT_FREE", dirname, msg="A directory of this name is already mounted")

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