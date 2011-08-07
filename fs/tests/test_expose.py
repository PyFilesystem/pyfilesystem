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
from fs.memoryfs import MemoryFS
from fs.path import *
from fs.errors import *

from fs import rpcfs
from fs.expose.xmlrpc import RPCFSServer
class TestRPCFS(unittest.TestCase, FSTestCases, ThreadingTestCases):
    
    def makeServer(self,fs,addr):
        return RPCFSServer(fs,addr,logRequests=False)

    def startServer(self):        
        port = 3000
        self.temp_fs = TempFS()
        self.server = None
        
        self.serve_more_requests = True
        self.server_thread = threading.Thread(target=self.runServer)
        self.server_thread.setDaemon(True) 
        
        self.start_event = threading.Event()
        self.end_event = threading.Event()
                   
        self.server_thread.start()
        
        self.start_event.wait()

    def runServer(self):
        """Run the server, swallowing shutdown-related execptions."""
        
        port = 3000
        while not self.server:
            try:
                self.server = self.makeServer(self.temp_fs,("127.0.0.1",port))
            except socket.error, e:
                if e.args[1] == "Address already in use":
                    port += 1
                else:
                    raise
        self.server_addr = ("127.0.0.1", port)
        
        self.server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
#        if sys.platform != "win32":
#            try:
#                self.server.socket.settimeout(1)
#            except socket.error:
#                pass
#        
        self.start_event.set()
        
        try:
            #self.server.serve_forever()
            while self.serve_more_requests:
                self.server.handle_request()
        except Exception, e:
            pass
        
        self.end_event.set()

    def setUp(self):
        self.startServer()
        self.fs = rpcfs.RPCFS("http://%s:%d" % self.server_addr)

    def tearDown(self):
        self.serve_more_requests = False
        #self.server.socket.close()
#            self.server.socket.shutdown(socket.SHUT_RDWR)
#            self.server.socket.close()
#            self.temp_fs.close()
        #self.server_thread.join()
        
        #self.end_event.wait()
        #return
        
        try:
            self.bump()
            self.server.server_close()
        except Exception:
            pass
        #self.server_thread.join()
        self.temp_fs.close()

    def bump(self):
        host, port = self.server_addr
        for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
            af, socktype, proto, cn, sa = res
            sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.settimeout(.1)
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
        self.fs = sftpfs.SFTPFS(self.server_addr, no_auth=True)
            
    #def runServer(self):
    #    self.server.serve_forever()
    #    
    #def tearDown(self):
    #    self.server.shutdown()
    #    self.server_thread.join()
    #    self.temp_fs.close()

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
            rawfs.setcontents("autoRun.inf","evilcodeevilcode")
            self.assertTrue(safefs.exists("_autoRun.inf"))
            self.assertTrue("autoRun.inf" not in safefs.listdir("/"))
            safefs.setcontents("file:stream","test")
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
