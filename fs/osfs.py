#!/usr/bin/env python

from fs import *

class OSFS(FS):

    def __init__(self, root_path):
        FS.__init__(self)

        expanded_path = normpath(os.path.expanduser(os.path.expandvars(root_path)))

        if not os.path.exists(expanded_path):
            raise ResourceNotFoundError("NO_DIR", expanded_path, msg="Root directory does not exist: %(path)s")
        if not os.path.isdir(expanded_path):
            raise ResourceNotFoundError("NO_DIR", expanded_path, msg="Root path is not a directory: %(path)s")

        self.root_path = normpath(os.path.abspath(expanded_path))

    def __str__(self):
        return "<OSFS \"%s\">" % self.root_path

    def getsyspath(self, path, allow_none=False):
        sys_path = os.path.join(self.root_path, makerelative(self._resolve(path)))
        return sys_path

    def open(self, path, mode="r", buffering=-1, **kwargs):
        try:
            f = open(self.getsyspath(path), mode, buffering)
        except IOError, e:
            raise OperationFailedError("OPEN_FAILED", path, details=e, msg=str(details))

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

    def ishidden(self, path):
        return path.startswith('.')

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):
        try:
            paths = os.listdir(self.getsyspath(path))
        except (OSError, IOError), e:
            raise OperationFailedError("LISTDIR_FAILED", path, details=e, msg="Unable to get directory listing: %(path)s - (%(details)s)")

        return self._listdir_helper(path, paths, wildcard, full, absolute, hidden, dirs_only, files_only)

    def makedir(self, path, mode=0777, recursive=False):
        sys_path = self.getsyspath(path)

        try:
            if recursive:
                os.makedirs(sys_path, mode)
            else:
                try:
                    os.mkdir(sys_path, mode)
                except OSError, e:
                    raise FSError("NO_DIR", sys_path)
        except OSError, e:
            raise FSError("OS_ERROR", sys_path, details=e)

    def remove(self, path):
        sys_path = self.getsyspath(path)
        try:
            os.remove(sys_path)
        except OSError, e:
            raise OperationFailedError("REMOVE_FAILED", path, details=e)

    def removedir(self, path, recursive=False):
        sys_path = self.getsyspath(path)

        if recursive:
            try:
                os.removedirs(sys_path)
            except OSError, e:
                raise OperationFailedError("REMOVEDIR_FAILED", path, details=e)
        else:
            try:
                os.rmdir(sys_path)
            except OSError, e:
                raise OperationFailedError("REMOVEDIR_FAILED", path, details=e)

    def rename(self, src, dst):
        path_src = self.getsyspath(src)
        path_dst = self.getsyspath(dst)

        try:
            os.rename(path_src, path_dst)
        except OSError, e:
            raise OperationFailedError("RENAME_FAILED", src)

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


    for p in osfs.walk("tagging-trunk", search='depth'):
        print p

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