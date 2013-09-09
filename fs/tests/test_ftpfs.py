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
import urllib

from six import PY3


try:
    from pyftpdlib.authorizers import DummyAuthorizer
    from pyftpdlib.handlers import FTPHandler
    from pyftpdlib.servers import FTPServer
except ImportError:
    if not PY3:
        raise ImportError("Requires pyftpdlib <http://code.google.com/p/pyftpdlib/>")

from fs.path import *

from fs import ftpfs

ftp_port = 30000
class TestFTPFS(unittest.TestCase, FSTestCases, ThreadingTestCases):

    __test__ = not PY3

    def setUp(self):
        global ftp_port
        ftp_port += 1
        use_port = str(ftp_port)
        #ftp_port = 10000
        self.temp_dir = tempfile.mkdtemp(u"ftpfstests")

        file_path = __file__
        if ':' not in file_path:
            file_path = abspath(file_path)
        # Apparently Windows requires values from default environment, so copy the exisiting os.environ
        env = os.environ.copy()
        env['PYTHONPATH'] = os.getcwd() + os.pathsep + env.get('PYTHONPATH', '')
        self.ftp_server = subprocess.Popen([sys.executable,
                                            file_path,
                                            self.temp_dir,
                                            use_port],
                                           stdout=subprocess.PIPE,
                                           env=env)
        # Block until the server writes a line to stdout
        self.ftp_server.stdout.readline()

        # Poll until a connection can be made
        start_time = time.time()
        while time.time() - start_time < 5:
            try:
                ftpurl = urllib.urlopen('ftp://127.0.0.1:%s' % use_port)
            except IOError:
                time.sleep(0)
            else:
                ftpurl.read()
                ftpurl.close()
                break
        else:
            # Avoid a possible infinite loop
            raise Exception("Unable to connect to ftp server")

        self.fs = ftpfs.FTPFS('127.0.0.1', 'user', '12345', dircache=True, port=use_port, timeout=5.0)
        self.fs.cache_hint(True)


    def tearDown(self):
        #self.ftp_server.terminate()
        if sys.platform == 'win32':
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
    authorizer = DummyAuthorizer()
    authorizer.add_user("user", "12345", sys.argv[1], perm="elradfmw")
    authorizer.add_anonymous(sys.argv[1])

    #def nolog(*args):
    #    pass
    #ftpserver.log = nolog
    #ftpserver.logline = nolog

    handler = FTPHandler
    handler.authorizer = authorizer
    address = ("127.0.0.1", int(sys.argv[2]))
    #print address

    ftpd = FTPServer(address, handler)

    sys.stdout.write('serving\n')
    sys.stdout.flush()
    ftpd.serve_forever()
