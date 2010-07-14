"""
fs.wrapfs.readonlyfs
====================

An FS wrapper class for blocking operations that would modify the FS.

"""

from fs.wrapfs import WrapFS
from fs.errors import UnsupportedError, NoSysPathError

class ReadOnlyFS(WrapFS):
    """ Makes a FS object read only. Any operation that could potentially modify
    the underlying file system will throw an UnsupportedError
    
    Note that this isn't a secure sandbox, untrusted code could work around the
    read-only restrictions by getting the base class. Its main purpose is to
    provide a degree of safety if you want to protect an FS object from
    accidental modification.
    
    """
        
    def getsyspath(self, path, allow_none=False):
        """ Doesn't technically modify the filesystem but could be used to work
        around read-only restrictions. """
        if allow_none:
            return None
        raise NoSysPathError(path)
    
    def open(self, path, mode='r', **kwargs):
        """ Only permit read access """
        if 'w' in mode or 'a' in mode:
            raise UnsupportedError('write')
        return super(ReadOnlyFS, self).open(path, mode, **kwargs)
        
    def _no_can_do(self, *args, **kwargs):
        """ Replacement method for methods that can modify the file system """
        raise UnsupportedError('write')
      
    move = _no_can_do
    movedir = _no_can_do
    copy = _no_can_do
    copydir = _no_can_do
    makedir = _no_can_do
    rename = _no_can_do
    setxattr = _no_can_do
    delattr = _no_can_do
    remove = _no_can_do
    removedir = _no_can_do
    settimes = _no_can_do
