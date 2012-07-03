"""
fs.wrapfs.hidefs
================

Removes resources from a directory listing if they match a given set of wildcards

"""

from fs.wrapfs import WrapFS
from fs.path import iteratepath
from fs.errors import ResourceNotFoundError
import re
import fnmatch


class HideFS(WrapFS):
    """FS wrapper that hides resources if they match a wildcard(s).

    For example, to hide all pyc file and subversion directories from a filesystem::

        hide_fs = HideFS(my_fs, "*.pyc", ".svn")

    """

    def __init__(self, wrapped_fs, *hide_wildcards):
        self._hide_wildcards = [re.compile(fnmatch.translate(wildcard)) for wildcard in hide_wildcards]
        super(HideFS, self).__init__(wrapped_fs)

    def _should_hide(self, path):
        return any(any(wildcard.match(part) for wildcard in self._hide_wildcards)
                for part in iteratepath(path))

    def _encode(self, path):
        if self._should_hide(path):
            raise ResourceNotFoundError(path)
        return path

    def _decode(self, path):
        return path

    def exists(self, path):
        if self._should_hide(path):
            return False
        return super(HideFS, self).exists(path)

    def listdir(self, path="", *args, **kwargs):
        entries = super(HideFS, self).listdir(path, *args, **kwargs)
        entries = [entry for entry in entries if not self._should_hide(entry)]
        return entries

if __name__ == "__main__":
    from fs.osfs import OSFS
    hfs = HideFS(OSFS('~/projects/pyfilesystem'), "*.pyc", ".svn")
    hfs.tree()
