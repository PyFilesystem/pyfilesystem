"""
fs.wrapfs.hidefs
================

Removes resources from a directory listing if they match a given set of wildcards 

"""

from fs.wrapfs import WrapFS
from fs.path import basename
import re
import fnmatch

class HideFS(WrapFS):
    """FS wrapper that hides resources if they match a wildcard(s).
    
    For example, to hide all pyc file and subversion directories from a filesystem::
    
        HideFS(my_fs, "*.pyc", ".svn")
    
    """
        
    def __init__(self, wrapped_fs, *hide_wildcards):            
        self._hide_wildcards = [re.compile(fnmatch.translate(wildcard)) for wildcard in hide_wildcards]
        super(HideFS, self).__init__(wrapped_fs)
    
    def _should_hide(self, name):
        name = basename(name)
        return any(wildcard.match(name) for wildcard in self._hide_wildcards)
    
    def _encode(self, path):
        return path

    def _decode(self, path):
        return path

    def listdir(self, path="", *args, **kwargs):        
        entries = super(HideFS, self).listdir(path, *args, **kwargs)        
        entries = [entry for entry in entries if not self._should_hide(entry)]        
        return entries

if __name__ == "__main__":
    from fs.osfs import OSFS
    hfs = HideFS(OSFS('~/projects/pyfilesystem'), "*.pyc", ".svn")
    hfs.tree()    