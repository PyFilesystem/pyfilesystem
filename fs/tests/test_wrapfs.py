"""

  fs.tests.test_wrapfs:  testcases for FS wrapper implementations

"""

import unittest
from fs.tests import FSTestCases, ThreadingTestCases

import os
import sys
import shutil
import tempfile

from fs import osfs
from fs.errors import * 
from fs.path import *


from fs import wrapfs
class TestWrapFS(unittest.TestCase, FSTestCases, ThreadingTestCases):
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(u"fstest")
        self.fs = wrapfs.WrapFS(osfs.OSFS(self.temp_dir))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def check(self, p):
        return os.path.exists(os.path.join(self.temp_dir, relpath(p)))

from fs.wrapfs.limitsizefs import LimitSizeFS
class TestLimitSizeFS(TestWrapFS):

    _dont_retest = TestWrapFS._dont_retest + ("test_big_file",)
    
    def setUp(self):
        super(TestLimitSizeFS,self).setUp()
        self.fs = LimitSizeFS(self.fs,1024*1024*2)  # 2MB limit

    def tearDown(self):
        self.fs.removedir("/",force=True)
        self.assertEquals(self.fs.cur_size,0)
        super(TestLimitSizeFS,self).tearDown()

    def test_storage_error(self):
        total_written = 0
        for i in xrange(1024*2):
            try:
                total_written += 1030
                self.fs.setcontents("file"+str(i),"C"*1030)
            except StorageSpaceError:
                self.assertTrue(total_written > 1024*1024*2)
                self.assertTrue(total_written < 1024*1024*2 + 1030)
                break
        else:
            self.assertTrue(False,"StorageSpaceError not raised")           

