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

from pyftpdlib import ftpserver

from fs.path import *
from fs.osfs import OSFS
from fs.errors import convert_fs_errors

# Get these once so we can reuse them:
UID = os.getuid()
GID = os.getgid()


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
    AbstractedFS. This class will cause the FTP server to service the given fs instance.
    """
    encoding = 'utf8'
    "Sets the encoding to use for paths."

    def __init__(self, fs, root, cmd_channel, encoding=None):
        self.fs = fs
        if encoding is not None:
            self.encoding = encoding
        super(FTPFS, self).__init__(root, cmd_channel)

    def validpath(self, path):
        try:
            normpath(path)
            return True
        except:
            return False

    @convert_fs_errors
    def open(self, path, mode):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        return self.fs.open(path, mode)

    def chdir(self, path):
        self._cwd = self.ftp2fs(path)

    @convert_fs_errors
    def mkdir(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        self.fs.makedir(path)

    @convert_fs_errors
    def listdir(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        return map(lambda x: x.encode(self.encoding), self.fs.listdir(path))

    @convert_fs_errors
    def rmdir(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        self.fs.removedir(path)

    @convert_fs_errors
    def remove(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        self.fs.remove(path)

    @convert_fs_errors
    def rename(self, src, dst):
        if not isinstance(src, unicode):
            src = src.decode(self.encoding)
        if not isinstance(dst, unicode):
            dst = dst.decode(self.encoding)
        self.fs.rename(src, dst)

    def chmod(self, path, mode):
        raise NotImplementedError()

    @convert_fs_errors
    def stat(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        info = self.fs.getinfo(path)
        kwargs = {
            'st_size': info.get('size'),
            # Echo current user instead of 0/0.
            'st_uid': UID,
            'st_gid': GID,
        }
        if 'st_atime' in info:
            kwargs['st_atime'] = info.get('st_atime')
        elif 'accessed_time' in info:
            kwargs['st_atime'] = time.mktime(info.get("accessed_time").timetuple())
        if 'st_mtime' in info:
            kwargs['st_mtime'] = info.get('st_mtime')
        elif 'modified_time' in info:
            kwargs['st_mtime'] = time.mktime(info.get("modified_time").timetuple())
        # Pyftpdlib uses st_ctime on Windows platform, try to provide it.
        if 'st_ctime' in info:
            kwargs['st_ctime'] = info.get('st_ctime')
        elif 'created_time' in info:
            kwargs['st_ctime'] = time.mktime(info.get("created_time").timetuple())
        elif 'st_mtime' in kwargs:
            # As a last resort, just copy the modified time.
            kwargs['st_ctime'] = kwargs['st_mtime']
        if self.fs.isdir(path):
            kwargs['st_mode'] = 0777 | stat.S_IFDIR
        else:
            kwargs['st_mode'] = 0777 | stat.S_IFREG
        return FakeStat(**kwargs)

    # No link support...
    lstat = stat

    @convert_fs_errors
    def isfile(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        return self.fs.isfile(path)

    @convert_fs_errors
    def isdir(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        return self.fs.isdir(path)

    @convert_fs_errors
    def getsize(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        return self.fs.getsize(path)

    @convert_fs_errors
    def getmtime(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        return self.fs.getinfo(path).time

    def realpath(self, path):
        return path

    def lexists(self, path):
        return True


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
        return FTPFS(self.fs, root, cmd_channel, encoding=encoding)


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
    ftp_handler = ftpserver.FTPHandler
    ftp_handler.authorizer = ftpserver.DummyAuthorizer()
    ftp_handler.authorizer.add_anonymous('/')
    ftp_handler.abstracted_fs = FTPFSFactory(fs)
    s = ftpserver.FTPServer((addr, port), ftp_handler)
    s.serve_forever()
