#!/usr/bin/env python
from fs.tests import FSTestCases, ThreadingTestCases

import unittest

import os
import sys
import shutil
import tempfile
import subprocess
import time
from os.path import abspath

try:
    from pyftpdlib import ftpserver
except ImportError:
    raise ImportError("Requires pyftpdlib <http://code.google.com/p/pyftpdlib/>")

from fs.path import *

from fs import ftpfs

ftp_port = 30000
class TestFTPFS(unittest.TestCase, FSTestCases, ThreadingTestCases):

    def setUp(self):
        global ftp_port
        ftp_port += 1
        use_port = str(ftp_port)
        #ftp_port = 10000
        self.temp_dir = tempfile.mkdtemp(u"ftpfstests")

        file_path = __file__
        if ':' not in file_path:
            file_path = abspath(file_path)
        self.ftp_server = subprocess.Popen([sys.executable, file_path, self.temp_dir, str(use_port)])
        # Need to sleep to allow ftp server to start
        time.sleep(.2)
        self.fs = ftpfs.FTPFS('127.0.0.1', 'user', '12345', dircache=True, port=use_port, timeout=5.0)
        self.fs.cache_hint(True)


    def tearDown(self):
        if sys.platform == 'win32':
            import win32api
            os.popen('TASKKILL /PID '+str(self.ftp_server.pid)+' /F')
        else:
            os.system('kill '+str(self.ftp_server.pid))
        shutil.rmtree(self.temp_dir)
        self.fs.close()

    def check(self, p):
        check_path = self.temp_dir.rstrip(os.sep) + os.sep + p
        return os.path.exists(check_path.encode('utf-8'))


if __name__ == "__main__":

    # Run an ftp server that exposes a given directory
    import sys
    authorizer = ftpserver.DummyAuthorizer()    
    authorizer.add_user("user", "12345", sys.argv[1], perm="elradfmw")
    authorizer.add_anonymous(sys.argv[1])

    def nolog(*args):
        pass
    ftpserver.log = nolog
    ftpserver.logline = nolog

    handler = ftpserver.FTPHandler
    handler.authorizer = authorizer
    address = ("127.0.0.1", int(sys.argv[2]))
    #print address

    ftpd = ftpserver.FTPServer(address, handler)

    ftpd.serve_forever()
