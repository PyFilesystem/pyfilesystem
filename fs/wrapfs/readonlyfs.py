"""
fs.wrapfs.readonlyfs
====================

An FS wrapper class for blocking operations that would modify the FS.

"""

from fs.base import NoDefaultMeta
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

    def getmeta(self, meta_name, default=NoDefaultMeta):
        if meta_name == 'read_only':
            return True
        return self.wrapped_fs.getmeta(meta_name, default)

    def hasmeta(self, meta_name):
        if meta_name == 'read_only':
            return True
        return self.wrapped_fs.hasmeta(meta_name)

    def getsyspath(self, path, allow_none=False):
        """ Doesn't technically modify the filesystem but could be used to work
        around read-only restrictions. """
        if allow_none:
            return None
        raise NoSysPathError(path)

    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        """ Only permit read access """
        if 'w' in mode or 'a' in mode or '+' in mode:
            raise UnsupportedError('write')
        return super(ReadOnlyFS, self).open(path,
                                            mode=mode,
                                            buffering=buffering,
                                            encoding=encoding,
                                            errors=errors,
                                            newline=newline,
                                            line_buffering=line_buffering,
                                            **kwargs)

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
    delxattr = _no_can_do
    remove = _no_can_do
    removedir = _no_can_do
    settimes = _no_can_do
    setcontents = _no_can_do
    createfile = _no_can_do
