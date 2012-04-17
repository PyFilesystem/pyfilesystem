"""
fs.expose.ftp
==============

Expose an FS object over FTP (via pyftpdlib).

This module provides the necessary interfaces to expose an FS object over
FTP, plugging into the infrastructure provided by the 'pyftpdlib' module.
"""

from __future__ import with_statement

import os
import stat as statinfo
import time
import threading

from pyftpdlib import ftpserver

from fs.base import flags_to_mode
from fs.path import *
from fs.errors import *
from fs.local_functools import wraps
from fs.filelike import StringIO
from fs.utils import isdir
from fs.osfs import OSFS

class FTPFS(ftpserver.AbstractedFS):
    def __init__(self, fs, root, cmd_channel):
        self.fs = fs
        super(FTPFS, self).__init__(root, cmd_channel)

    def validpath(self, path):
        return True

    def open(self, path, mode):
        return self.fs.open(path, mode)

    def chdir(self, path):
        self._cwd = self.ftp2fs(path)

    def mkdir(self, path):
        if isinstance(path, str):
            path = unicode(path, sys.getfilesystemencoding())
        self.fs.createdir(path)

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
    from pyftpdlib.contrib.authorizers import UnixAuthorizer
    ftp_handler = ftpserver.FTPHandler
    ftp_handler.authorizer = ftpserver.DummyAuthorizer()
    ftp_handler.authorizer.add_anonymous('/')
    ftp_handler.abstracted_fs = FTPFSFactory(fs)
    s = ftpserver.FTPServer((addr, port), ftp_handler)
    s.serve_forever()


def main():
    serve_fs(HomeFTPFS, '127.0.0.1', 21)


#  When called from the command-line, expose a DemoFS for testing purposes
if __name__ == "__main__":
    main()
