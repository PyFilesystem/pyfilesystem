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
from fs.utils import remove_all
from fs import wrapfs

import six
from six import PY3, b

class TestWrapFS(unittest.TestCase, FSTestCases, ThreadingTestCases):
    
    #__test__ = False
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(u"fstest")
        self.fs = wrapfs.WrapFS(osfs.OSFS(self.temp_dir))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.fs.close()

    def check(self, p):
        return os.path.exists(os.path.join(self.temp_dir, relpath(p)))


from fs.wrapfs.lazyfs import LazyFS
class TestLazyFS(unittest.TestCase, FSTestCases, ThreadingTestCases):
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(u"fstest")
        self.fs = LazyFS((osfs.OSFS,(self.temp_dir,)))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.fs.close()

    def check(self, p):
        return os.path.exists(os.path.join(self.temp_dir, relpath(p)))


from fs.wrapfs.limitsizefs import LimitSizeFS
class TestLimitSizeFS(TestWrapFS):

    _dont_retest = TestWrapFS._dont_retest + ("test_big_file",)
    
    def setUp(self):
        super(TestLimitSizeFS,self).setUp()
        self.fs = LimitSizeFS(self.fs,1024*1024*2)  # 2MB limit

    def tearDown(self):
        remove_all(self.fs, "/")
        self.assertEquals(self.fs.cur_size,0)
        super(TestLimitSizeFS,self).tearDown()
        self.fs.close()

    def test_storage_error(self):
        total_written = 0
        for i in xrange(1024*2):
            try:
                total_written += 1030
                self.fs.setcontents("file %i" % i, b("C")*1030)
            except StorageSpaceError:
                self.assertTrue(total_written > 1024*1024*2)
                self.assertTrue(total_written < 1024*1024*2 + 1030)
                break
        else:
            self.assertTrue(False,"StorageSpaceError not raised")           


from fs.wrapfs.hidedotfilesfs import HideDotFilesFS
class TestHideDotFilesFS(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(u"fstest")
        open(os.path.join(self.temp_dir, u".dotfile"), 'w').close()
        open(os.path.join(self.temp_dir, u"regularfile"), 'w').close()
        os.mkdir(os.path.join(self.temp_dir, u".dotdir"))
        os.mkdir(os.path.join(self.temp_dir, u"regulardir"))
        self.fs = HideDotFilesFS(osfs.OSFS(self.temp_dir))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.fs.close()

    def test_hidden(self):
        self.assertEquals(len(self.fs.listdir(hidden=False)), 2)
        self.assertEquals(len(list(self.fs.ilistdir(hidden=False))), 2)

    def test_nonhidden(self):
        self.assertEquals(len(self.fs.listdir(hidden=True)), 4)
        self.assertEquals(len(list(self.fs.ilistdir(hidden=True))), 4)

    def test_default(self):
        self.assertEquals(len(self.fs.listdir()), 2)
        self.assertEquals(len(list(self.fs.ilistdir())), 2)


