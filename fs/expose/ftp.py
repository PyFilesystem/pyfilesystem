"""
fs.expose.ftp
==============

Expose an FS object over FTP (via pyftpdlib).

This module provides the necessary interfaces to expose an FS object over
FTP, plugging into the infrastructure provided by the 'pyftpdlib' module.

To use this in combination with fsserve, do the following:

$ fsserve -t 'ftp' $HOME

The above will serve your home directory in read-only mode via anonymous FTP on the
loopback address.
"""

import os
import stat
import time
import errno
from functools import wraps

from pyftpdlib import ftpserver

from fs.path import *
from fs.osfs import OSFS
from fs.errors import convert_fs_errors
from fs import iotools

from six import text_type as unicode


# Get these once so we can reuse them:
UID = os.getuid()
GID = os.getgid()


def decode_args(f):
    """
    Decodes string arguments using the decoding defined on the method's class.
    This decorator is for use on methods (functions which take a class or instance
    as the first parameter).

    Pyftpdlib (as of 0.7.0) uses str internally, so this decoding is necessary.
    """
    @wraps(f)
    def wrapper(self, *args):
        encoded = []
        for arg in args:
            if isinstance(arg, str):
                arg = arg.decode(self.encoding)
            encoded.append(arg)
        return f(self, *encoded)
    return wrapper


class FakeStat(object):
    """
    Pyftpdlib uses stat inside the library. This class emulates the standard
    os.stat_result class to make pyftpdlib happy. Think of it as a stat-like
    object ;-).
    """
    def __init__(self, **kwargs):
        for attr in dir(stat):
            if not attr.startswith('ST_'):
                continue
            attr = attr.lower()
            value = kwargs.get(attr, 0)
            setattr(self, attr, value)


class FTPFS(ftpserver.AbstractedFS):
    """
    The basic FTP Filesystem. This is a bridge between a pyfs filesystem and pyftpdlib's
    AbstractedFS. This class will cause the FTP server to serve the given fs instance.
    """
    encoding = 'utf8'
    "Sets the encoding to use for paths."

    def __init__(self, fs, root, cmd_channel, encoding=None):
        self.fs = fs
        if encoding is not None:
            self.encoding = encoding
        super(FTPFS, self).__init__(root, cmd_channel)

    def close(self):
        # Close and dereference the pyfs file system.
        if self.fs:
            self.fs.close()
        self.fs = None

    def validpath(self, path):
        try:
            normpath(path)
            return True
        except:
            return False

    @convert_fs_errors
    @decode_args
    @iotools.filelike_to_stream
    def open(self, path, mode, **kwargs):
        return self.fs.open(path, mode, **kwargs)

    @convert_fs_errors
    def chdir(self, path):
        # We dont' use the decorator here, we actually decode a version of the
        # path for use with pyfs, but keep the original for use with pyftpdlib.
        if not isinstance(path, unicode):
            # pyftpdlib 0.7.x
            unipath = unicode(path, self.encoding)
        else:
            # pyftpdlib 1.x
            unipath = path
        # TODO: can the following conditional checks be farmed out to the fs?
        # If we don't raise an error here for files, then the FTP server will
        # happily allow the client to CWD into a file. We really only want to
        # allow that for directories.
        if self.fs.isfile(unipath):
            raise OSError(errno.ENOTDIR, 'Not a directory')
        # similarly, if we don't check for existence, the FTP server will allow
        # the client to CWD into a non-existent directory.
        if not self.fs.exists(unipath):
            raise OSError(errno.ENOENT, 'Does not exist')
        # We use the original path here, so we don't corrupt self._cwd
        self._cwd = self.ftp2fs(path)

    @convert_fs_errors
    @decode_args
    def mkdir(self, path):
        self.fs.makedir(path)

    @convert_fs_errors
    @decode_args
    def listdir(self, path):
        return map(lambda x: x.encode(self.encoding), self.fs.listdir(path))

    @convert_fs_errors
    @decode_args
    def rmdir(self, path):
        self.fs.removedir(path)

    @convert_fs_errors
    @decode_args
    def remove(self, path):
        self.fs.remove(path)

    @convert_fs_errors
    @decode_args
    def rename(self, src, dst):
        self.fs.rename(src, dst)

    @convert_fs_errors
    @decode_args
    def chmod(self, path, mode):
        return

    @convert_fs_errors
    @decode_args
    def stat(self, path):
        info = self.fs.getinfo(path)
        kwargs = {
            'st_size': info.get('size'),
        }
        # Give the fs a chance to provide the uid/gid. Otherwise echo the current
        # uid/gid.
        kwargs['st_uid'] = info.get('st_uid', UID)
        kwargs['st_gid'] = info.get('st_gid', GID)
        if 'st_atime' in info:
            kwargs['st_atime'] = info['st_atime']
        elif 'accessed_time' in info:
            kwargs['st_atime'] = time.mktime(info["accessed_time"].timetuple())
        if 'st_mtime' in info:
            kwargs['st_mtime'] = info.get('st_mtime')
        elif 'modified_time' in info:
            kwargs['st_mtime'] = time.mktime(info["modified_time"].timetuple())
        # Pyftpdlib uses st_ctime on Windows platform, try to provide it.
        if 'st_ctime' in info:
            kwargs['st_ctime'] = info['st_ctime']
        elif 'created_time' in info:
            kwargs['st_ctime'] = time.mktime(info["created_time"].timetuple())
        elif 'st_mtime' in kwargs:
            # As a last resort, just copy the modified time.
            kwargs['st_ctime'] = kwargs['st_mtime']
        # Try to use existing mode.
        if 'st_mode' in info:
            kwargs['st_mode'] = info['st_mode']
        elif 'mode' in info:
            kwargs['st_mode'] = info['mode']
        else:
            # Otherwise, build one. Not executable by default.
            mode = 0660
            # Merge in the type (dir or file). File is tested first, some file systems
            # such as ArchiveMountFS treat archive files as directories too. By checking
            # file first, any such files will be only files (not directories).
            if self.fs.isfile(path):
                mode |= stat.S_IFREG
            elif self.fs.isdir(path):
                mode |= stat.S_IFDIR
                mode |= 0110  # Merge in exec bit to signal dir is listable
            kwargs['st_mode'] = mode
        return FakeStat(**kwargs)

    # No link support...
    lstat = stat

    @convert_fs_errors
    @decode_args
    def isfile(self, path):
        return self.fs.isfile(path)

    @convert_fs_errors
    @decode_args
    def isdir(self, path):
        return self.fs.isdir(path)

    @convert_fs_errors
    @decode_args
    def getsize(self, path):
        return self.fs.getsize(path)

    @convert_fs_errors
    @decode_args
    def getmtime(self, path):
        return self.stat(path).st_mtime

    def realpath(self, path):
        return path

    def lexists(self, path):
        return True


class FTPFSHandler(ftpserver.FTPHandler):
    """
    An FTPHandler class that closes the filesystem when done.
    """

    def close(self):
        # Close the FTPFS instance, it will close the pyfs file system.
        if self.fs:
            self.fs.close()
        super(FTPFSHandler, self).close()


class FTPFSFactory(object):
    """
    A factory class which can hold a reference to a file system object and
    encoding, then later pass it along to an FTPFS instance. An instance of
    this object allows multiple FTPFS instances to be created by pyftpdlib
    while sharing the same fs.
    """
    def __init__(self, fs, encoding=None):
        """
        Initializes the factory with an fs instance.
        """
        self.fs = fs
        self.encoding = encoding

    def __call__(self, root, cmd_channel):
        """
        This is the entry point of pyftpdlib. We will pass along the two parameters
        as well as the previously provided fs instance and encoding.
        """
        return FTPFS(self.fs, root, cmd_channel, encoding=self.encoding)


class HomeFTPFS(FTPFS):
    """
    A file system which serves a user's home directory.
    """
    def __init__(self, root, cmd_channel):
        """
        Use the provided user's home directory to create an FTPFS that serves an OSFS
        rooted at the home directory.
        """
        super(DemoFS, self).__init__(OSFS(root_path=root), '/', cmd_channel)


def serve_fs(fs, addr, port):
    """
    Creates a basic anonymous FTP server serving the given FS on the given address/port
    combo.
    """
    from pyftpdlib.contrib.authorizers import UnixAuthorizer
    ftp_handler = FTPFSHandler
    ftp_handler.authorizer = ftpserver.DummyAuthorizer()
    ftp_handler.authorizer.add_anonymous('/')
    ftp_handler.abstracted_fs = FTPFSFactory(fs)
    s = ftpserver.FTPServer((addr, port), ftp_handler)
    s.serve_forever()
