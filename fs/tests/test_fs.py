"""

  fs.tests.test_fs:  testcases for basic FS implementations

"""

from fs.tests import FSTestCases, ThreadingTestCases
from fs.path import *
from fs import errors

import unittest

import os
import sys
import shutil
import tempfile


from fs import osfs
class TestOSFS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(u"fstest")
        self.fs = osfs.OSFS(self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.fs.close()

    def check(self, p):
        return os.path.exists(os.path.join(self.temp_dir, relpath(p)))

    def test_invalid_chars(self):
        super(TestOSFS, self).test_invalid_chars()

        self.assertRaises(errors.InvalidCharsInPathError, self.fs.open, 'invalid\0file', 'wb')
        self.assertFalse(self.fs.isvalidpath('invalid\0file'))
        self.assert_(self.fs.isvalidpath('validfile'))
        self.assert_(self.fs.isvalidpath('completely_valid/path/foo.bar'))


class TestSubFS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(u"fstest")
        self.parent_fs = osfs.OSFS(self.temp_dir)
        self.parent_fs.makedir("foo/bar", recursive=True)
        self.fs = self.parent_fs.opendir("foo/bar")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.fs.close()

    def check(self, p):
        p = os.path.join("foo/bar", relpath(p))
        full_p = os.path.join(self.temp_dir, p)
        return os.path.exists(full_p)


from fs import memoryfs
class TestMemoryFS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        self.fs = memoryfs.MemoryFS()


from fs import mountfs
class TestMountFS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        self.mount_fs = mountfs.MountFS()
        self.mem_fs = memoryfs.MemoryFS()
        self.mount_fs.mountdir("mounted/memfs", self.mem_fs)
        self.fs = self.mount_fs.opendir("mounted/memfs")

    def tearDown(self):
        self.fs.close()

    def check(self, p):
        return self.mount_fs.exists(pathjoin("mounted/memfs", relpath(p)))

class TestMountFS_atroot(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        self.mem_fs = memoryfs.MemoryFS()
        self.fs = mountfs.MountFS()
        self.fs.mountdir("", self.mem_fs)

    def tearDown(self):
        self.fs.close()

    def check(self, p):
        return self.mem_fs.exists(p)

class TestMountFS_stacked(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        self.mem_fs1 = memoryfs.MemoryFS()
        self.mem_fs2 = memoryfs.MemoryFS()
        self.mount_fs = mountfs.MountFS()
        self.mount_fs.mountdir("mem", self.mem_fs1)
        self.mount_fs.mountdir("mem/two", self.mem_fs2)
        self.fs = self.mount_fs.opendir("/mem/two")

    def tearDown(self):
        self.fs.close()

    def check(self, p):
        return self.mount_fs.exists(pathjoin("mem/two", relpath(p)))


from fs import tempfs
class TestTempFS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        self.fs = tempfs.TempFS()

    def tearDown(self):
        td = self.fs._temp_dir
        self.fs.close()
        self.assert_(not os.path.exists(td))

    def check(self, p):
        td = self.fs._temp_dir
        return os.path.exists(os.path.join(td, relpath(p)))

    def test_invalid_chars(self):
        super(TestTempFS, self).test_invalid_chars()

        self.assertRaises(errors.InvalidCharsInPathError, self.fs.open, 'invalid\0file', 'wb')
        self.assertFalse(self.fs.isvalidpath('invalid\0file'))
        self.assert_(self.fs.isvalidpath('validfile'))
        self.assert_(self.fs.isvalidpath('completely_valid/path/foo.bar'))
