"""

  fs.tests.test_expose:  testcases for fs.expose and associated FS classes

"""

import unittest
import sys
import os, os.path
import socket
import threading
import time

from fs.tests import FSTestCases, ThreadingTestCases
from fs.tempfs import TempFS
from fs.osfs import OSFS
from fs.path import *

from fs import rpcfs
from fs.expose.xmlrpc import RPCFSServer
class TestRPCFS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def makeServer(self,fs,addr):
        return RPCFSServer(fs,addr,logRequests=False)

    def startServer(self):
        port = 8000
        self.temp_fs = TempFS()
        self.server = None
        while not self.server:
            try:
                self.server = self.makeServer(self.temp_fs,("localhost",port))
            except socket.error, e:
                if e.args[1] == "Address already in use":
                    port += 1
                else:
                    raise
        self.server_addr = ("localhost",port)
        self.serve_more_requests = True
        self.server_thread = threading.Thread(target=self.runServer)
        self.server_thread.start()

    def runServer(self):
        """Run the server, swallowing shutdown-related execptions."""
        if sys.platform != "win32":
            try:
                self.server.socket.settimeout(0.1)
            except socket.error:
                pass
        try:
            while self.serve_more_requests:
                self.server.handle_request()
        except Exception, e:
            pass

    def setUp(self):
        self.startServer()
        self.fs = rpcfs.RPCFS("http://%s:%d" % self.server_addr)

    def tearDown(self):
        self.serve_more_requests = False
        try:
            self.bump()
            self.server.server_close()
        except Exception:
            pass
        self.server_thread.join()
        self.temp_fs.close()

    def bump(self):
        host, port = self.server_addr
        for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
            af, socktype, proto, cn, sa = res
            sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.settimeout(1)
                sock.connect(sa)
                sock.send("\n")
            except socket.error, e:
                pass
            finally:
                if sock is not None:
                    sock.close()


from fs import sftpfs
from fs.expose.sftp import BaseSFTPServer
class TestSFTPFS(TestRPCFS):

    def makeServer(self,fs,addr):
        return BaseSFTPServer(addr,fs)

    def setUp(self):
        self.startServer()
        self.fs = sftpfs.SFTPFS(self.server_addr)

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
    class TestFUSE(unittest.TestCase,FSTestCases,ThreadingTestCases):

        def setUp(self):
            self.temp_fs = TempFS()
            self.temp_fs.makedir("root")
            self.temp_fs.makedir("mount")
            self.mounted_fs = self.temp_fs.opendir("root")
            self.mount_point = self.temp_fs.getsyspath("mount")
            self.fs = OSFS(self.temp_fs.getsyspath("mount"))
            self.mount_proc = fuse.mount(self.mounted_fs,self.mount_point)

        def tearDown(self):
            self.mount_proc.unmount()
            try:
                self.temp_fs.close()
            except OSError:
                # Sometimes FUSE hangs onto the mountpoint if mount_proc is
                # forcibly killed.  Shell out to fusermount to make sure.
                fuse.unmount(self.mount_point)
                self.temp_fs.close()

        def check(self,p):
            return self.mounted_fs.exists(p)

