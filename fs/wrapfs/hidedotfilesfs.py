"""
fs.wrapfs.hidedotfilesfs
========================

An FS wrapper class for hiding dot-files in directory listings.

"""

from fs.wrapfs import WrapFS
from fs.path import *
from fnmatch import fnmatch


class HideDotFilesFS(WrapFS):
    """FS wrapper class that hides dot-files in directory listings.

    The listdir() function takes an extra keyword argument 'hidden'
    indicating whether hidden dot-files should be included in the output.
    It is False by default.
    """

    def is_hidden(self, path):
        """Check whether the given path should be hidden."""
        return path and basename(path)[0] == "."

    def _encode(self, path):
        return path

    def _decode(self, path):
        return path

    def listdir(self, path="", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False, hidden=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        entries = self.wrapped_fs.listdir(path,**kwds)
        if not hidden:
            entries = [e for e in entries if not self.is_hidden(e)]
        return entries

    def ilistdir(self, path="", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False, hidden=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        for e in self.wrapped_fs.ilistdir(path,**kwds):
            if hidden or not self.is_hidden(e):
                yield e

    def walk(self, path="/", wildcard=None, dir_wildcard=None, search="breadth",hidden=False):
        if search == "breadth":
            dirs = [path]
            while dirs:
                current_path = dirs.pop()
                paths = []
                for filename in self.listdir(current_path,hidden=hidden):
                    path = pathjoin(current_path, filename)
                    if self.isdir(path):
                        if dir_wildcard is not None:
                            if fnmatch(path, dir_wildcard):
                                dirs.append(path)
                        else:
                            dirs.append(path)
                    else:
                        if wildcard is not None:
                            if fnmatch(path, wildcard):
                                paths.append(filename)
                        else:
                            paths.append(filename)
                yield (current_path, paths)
        elif search == "depth":
            def recurse(recurse_path):
                for path in self.listdir(recurse_path, wildcard=dir_wildcard, full=True, dirs_only=True,hidden=hidden):
                    for p in recurse(path):
                        yield p
                yield (recurse_path, self.listdir(recurse_path, wildcard=wildcard, files_only=True,hidden=hidden))
            for p in recurse(path):
                yield p
        else:
            raise ValueError("Search should be 'breadth' or 'depth'")


    def isdirempty(self, path):
        path = normpath(path)
        iter_dir = iter(self.listdir(path,hidden=True))
        try:
            iter_dir.next()
        except StopIteration:
            return True
        return False


