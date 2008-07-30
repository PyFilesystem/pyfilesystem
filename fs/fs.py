#!/usr/bin/env python

import os
import os.path
import fnmatch
from itertools import chain
import datetime


error_msgs = {

    "UNKNOWN_ERROR" :   "No information on error: %(path)s",
    "UNSUPPORTED" :     "Action is unsupported by this filesystem.",
    "INVALID_PATH" :    "Path is invalid: %(path)s",
    "NO_DIR" :          "Directory does not exist: %(path)s",
    "NO_FILE" :         "No such file: %(path)s",
    "NO_RESOURCE" :     "No path to: %(path)s",
    "LISTDIR_FAILED" :  "Unable to get directory listing: %(path)s",
    "DELETE_FAILED" :   "Unable to delete file: %(path)s",
    "NO_SYS_PATH" :     "No mapping to OS filesytem: %(path)s,",
    "DIR_EXISTS" :      "Directory exists (try allow_recreate=True): %(path)s",
    "OPEN_FAILED" :     "Unable to open file: %(path)s",
    "FILE_LOCKED" :     "File is locked: %(path)s",
}

error_codes = error_msgs.keys()

class FSError(Exception):

    def __init__(self, code, path=None, msg=None, details=None):

        self.code = code
        self.msg = msg or error_msgs.get(code, error_msgs['UNKNOWN_ERROR'])
        self.path = path
        self.details = details

    def __str__(self):

        msg = self.msg % dict((k, str(v)) for k, v in self.__dict__.iteritems())

        return '%s. %s' % (self.code, msg)


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
        except FSError, e:
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

    def getsyspath(self, path):

        raise FSError("NO_SYS_PATH", path)
    
    def safeopen(self, *args, **kwargs):
        
        try:
            f = self.open(*args, **kwargs)
        except FSError, e:
            if e.code == "NO_FILE":
                return NullFile()
            raise
      
    def desc(self, path):
        
        if not self.exists(path):        
            return "No description available"
                
        try:
            sys_path = self.getsyspath(path)
        except FSError:
            return "No description available"
        
        if self.isdir(path):
            return "OS dir, maps to %s" % sys_path
        else:
            return "OS file, maps to %s" % sys_path

    def open(self, path, mode="r", buffering=-1, **kwargs):

        pass

    def opendir(self, path):

        if not self.exists(path):
            raise FSError("NO_DIR", path)

        sub_fs = SubFS(self, path)
        return sub_fs


    def remove(self, path):

        raise FSError("UNSUPPORTED", path)


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


    def walk_files(self, path="/", wildcard=None, dir_wildcard=None):

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

    def __init__(self, parent, sub_dir):

        self.parent = parent
        self.sub_dir = parent.abspath(sub_dir)

    def __str__(self):
        return "<SubFS \"%s\" of %s>" % (self.sub_dir, self.parent)

    def _delegate(self, dirname):

        delegate_path = pathjoin(self.sub_dir, resolvepath(makerelative(dirname)))
        return delegate_path

    def getsyspath(self, pathname):

        return self.parent.getsyspath(self._delegate(pathname))

    def open(self, pathname, mode="r", buffering=-1, **kwargs):

        return self.parent.open(self._delegate(pathname), mode, buffering)

    def open_dir(self, path):

        if not self.exists(dirname):
            raise FSError("NO_DIR", dirname)

        path = self._delegate(path)
        sub_fs = self.parent.open_dir(path)
        return sub_fs

    def isdir(self, pathname):

        return self.parent.isdir(self._delegate(pathname))

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        return self.parent.listdir(self._delegate(path), wildcard, full, absolute, hidden, dirs_only, files_only)


class OSFS(FS):

    def __init__(self, root_path):

        expanded_path = normpath(os.path.expanduser(os.path.expandvars(root_path)))

        if not os.path.exists(expanded_path):
            raise FSError("NO_DIR", expanded_path, msg="Root directory does not exist: %(path)s")
        if not os.path.isdir(expanded_path):
            raise FSError("NO_DIR", expanded_path, msg="Root path is not a directory: %(path)s")

        self.root_path = normpath(os.path.abspath(expanded_path))

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
        except (OSError, IOError), e:
            raise FSError("LISTDIR_FAILED", path, details=e, msg="Unable to get directory listing: %(path)s - (%(details)s)")

        return self._listdir_helper(path, paths, wildcard, full, absolute, hidden, dirs_only, files_only)


    def mkdir(self, path, mode=0777, recursive=False):

        sys_path = self.getsyspath(path)

        if recursive:
            os.makedirs(sys_path, mode)
        else:
            os.makedir(sys_path, mode)


    def remove(self, path):

        sys_path = self.getsyspath(path)
        try:
            os.remove(sys_path)
        except OSError, e:
            raise FSError("FILE_DELETE_FAILED", path, details=e)


    def removedir(self, path, recursive=False):

        sys_path = self.getsyspath(path)

        if recursive:

            try:
                os.rmdir(sys_path)
            except OSError, e:
                raise FSError("DIR_DELETE_FAILED", path, details=e)

        else:

            try:
                os.removedirs(sys_path)
            except OSError, e:
                raise FSError("DIR_DELETE_FAILED", path, details=e)


    def getinfo(self, path):

        sys_path = self.getsyspath(path)

        try:
            stats = os.stat(sys_path)
        except OSError, e:
            raise FSError("UNKNOWN_ERROR", path, details=e)

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
            raise FSError("UNKNOWN_ERROR", path, details=e)

        return stats.st_size




if __name__ == "__main__":

    osfs = OSFS("~/projects")
    print osfs

    for filename in osfs.walk_files("/", "*.pov"):
        print filename
        print osfs.getinfo(filename)

    import browsewin
    browsewin.browse(osfs)

    #print_fs(osfs)

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