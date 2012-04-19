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

from pyftpdlib import ftpserver

from fs.osfs import OSFS


class FTPFS(ftpserver.AbstractedFS):
    """
    The basic FTP Filesystem. This is a bridge between a pyfs filesystem and pyftpdlib's
    AbstractedFS. This class will cause the FTP server to service the given fs instance.
    """
    def __init__(self, fs, root, cmd_channel):
        self.fs = fs
        super(FTPFS, self).__init__(root, cmd_channel)

    def validpath(self, path):
        # All paths are valid because we offload chrooting to pyfs.
        return True

    def open(self, path, mode):
        return self.fs.open(path, mode)

    def chdir(self, path):
        # Put the user into the requested directory, again, all paths
        # are valid.
        self._cwd = self.ftp2fs(path)

    def mkdir(self, path):
        self.fs.makedir(path)

    def listdir(self, path):
        return map(lambda x: x.encode('utf8'), self.fs.listdir(path))

    def rmdir(self, path):
        self.fs.removedir(path)

    def remove(self, path):
        self.fs.remove(path)

    def rename(self, src, dst):
        self.fs.rename(src, dst)

    def chmod(self, path, mode):
        raise NotImplementedError()

    def stat(self, path):
        # TODO: stat needs to be handled using fs.getinfo() method.
        return super(FTPFS, self).stat(self.fs.getsyspath(path))

    def lstat(self, path):
        return self.stat(path)

    def isfile(self, path):
        return self.fs.isfile(path)

    def isdir(self, path):
        return self.fs.isdir(path)

    def getsize(self, path):
        return self.fs.getsize(path)

    def getmtime(self, path):
        return self.fs.getinfo(path).time

    def realpath(self, path):
        return path

    def lexists(self, path):
        return True


class FTPFSFactory(object):
    """
    A factory class which can hold a reference to a file system object and
    later pass it along to an FTPFS instance. An instance of this object allows
    multiple FTPFS instances to be created by pyftpdlib and share the same fs.
    """
    def __init__(self, fs):
        """
        Initializes the factory with an fs instance.
        """
        self.fs = fs

    def __call__(self, root, cmd_channel):
        """
        This is the entry point of pyftpdlib. We will pass along the two parameters
        as well as the previously provided fs instance.
        """
        return FTPFS(self.fs, root, cmd_channel)


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
