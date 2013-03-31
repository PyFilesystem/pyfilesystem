"""

  fs.tests.test_expose:  testcases for fs.expose and associated FS classes

"""

import unittest
import sys
import os
import os.path
import socket
import threading
import time

from fs.tests import FSTestCases, ThreadingTestCases
from fs.tempfs import TempFS
from fs.osfs import OSFS
from fs.memoryfs import MemoryFS
from fs.path import *
from fs.errors import *

from fs import rpcfs
from fs.expose.xmlrpc import RPCFSServer

import six
from six import PY3, b

from fs.tests.test_rpcfs import TestRPCFS

try:
    from fs import sftpfs
    from fs.expose.sftp import BaseSFTPServer
except ImportError:
    if not PY3:
        raise

import logging
logging.getLogger('paramiko').setLevel(logging.ERROR)
logging.getLogger('paramiko.transport').setLevel(logging.ERROR)


class TestSFTPFS(TestRPCFS):

    __test__ = not PY3

    def makeServer(self,fs,addr):
        return BaseSFTPServer(addr,fs)

    def setUp(self):
        self.startServer()
        self.fs = sftpfs.SFTPFS(self.server_addr, no_auth=True)

    def bump(self):
        # paramiko doesn't like being bumped, just wait for it to timeout.
        # TODO: do this using a paramiko.Transport() connection
        pass


try:
    from fs.expose import fuse
except ImportError:
    pass
else:
    from fs.osfs import OSFS
    class TestFUSE(unittest.TestCase, FSTestCases, ThreadingTestCases):

        def setUp(self):
            self.temp_fs = TempFS()
            self.temp_fs.makedir("root")
            self.temp_fs.makedir("mount")
            self.mounted_fs = self.temp_fs.opendir("root")
            self.mount_point = self.temp_fs.getsyspath("mount")
            self.fs = OSFS(self.temp_fs.getsyspath("mount"))
            self.mount_proc = fuse.mount(self.mounted_fs, self.mount_point)

        def tearDown(self):
            self.mount_proc.unmount()
            try:
                self.temp_fs.close()
            except OSError:
                # Sometimes FUSE hangs onto the mountpoint if mount_proc is
                # forcibly killed.  Shell out to fusermount to make sure.
                fuse.unmount(self.mount_point)
                self.temp_fs.close()

        def check(self, p):
            return self.mounted_fs.exists(p)


from fs.expose import dokan
if dokan.is_available:
    from fs.osfs import OSFS
    class DokanTestCases(FSTestCases):
        """Specialised testcases for filesystems exposed via Dokan.

        This modifies some of the standard tests to work around apparent
        bugs in the current Dokan implementation.
        """

        def test_remove(self):
            self.fs.createfile("a.txt")
            self.assertTrue(self.check("a.txt"))
            self.fs.remove("a.txt")
            self.assertFalse(self.check("a.txt"))
            self.assertRaises(ResourceNotFoundError,self.fs.remove,"a.txt")
            self.fs.makedir("dir1")
            #  This appears to be a bug in Dokan - DeleteFile will happily
            #  delete an empty directory.
            #self.assertRaises(ResourceInvalidError,self.fs.remove,"dir1")
            self.fs.createfile("/dir1/a.txt")
            self.assertTrue(self.check("dir1/a.txt"))
            self.fs.remove("dir1/a.txt")
            self.assertFalse(self.check("/dir1/a.txt"))

        def test_open_on_directory(self):
            #  Dokan seems quite happy to ask me to open a directory and
            #  then treat it like a file.
            pass

        def test_settimes(self):
            #  Setting the times does actually work, but there's some sort
            #  of caching effect which prevents them from being read back
            #  out.  Disabling the test for now.
            pass

        def test_safety_wrapper(self):
            rawfs = MemoryFS()
            safefs = dokan.Win32SafetyFS(rawfs)
            rawfs.setcontents("autoRun.inf", b("evilcodeevilcode"))
            self.assertTrue(safefs.exists("_autoRun.inf"))
            self.assertTrue("autoRun.inf" not in safefs.listdir("/"))
            safefs.setcontents("file:stream",b("test"))
            self.assertFalse(rawfs.exists("file:stream"))
            self.assertTrue(rawfs.exists("file__colon__stream"))
            self.assertTrue("file:stream" in safefs.listdir("/"))

    class TestDokan(unittest.TestCase,DokanTestCases,ThreadingTestCases):

        def setUp(self):
            self.temp_fs = TempFS()
            self.drive = "K"
            while os.path.exists(self.drive+":\\") and self.drive <= "Z":
                self.drive = chr(ord(self.drive) + 1)
            if self.drive > "Z":
                raise RuntimeError("no free drive letters")
            fs_to_mount = OSFS(self.temp_fs.getsyspath("/"))
            self.mount_proc = dokan.mount(fs_to_mount,self.drive)#,flags=dokan.DOKAN_OPTION_DEBUG|dokan.DOKAN_OPTION_STDERR,numthreads=1)
            self.fs = OSFS(self.mount_proc.path)

        def tearDown(self):
            self.mount_proc.unmount()
            for _ in xrange(10):
                try:
                    if self.mount_proc.poll() is None:
                        self.mount_proc.terminate()
                except EnvironmentError:
                    time.sleep(0.1)
                else:
                    break
            else:
                if self.mount_proc.poll() is None:
                    self.mount_proc.terminate()
            self.temp_fs.close()

if __name__ == '__main__':
    unittest.main()
