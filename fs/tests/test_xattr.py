"""

  fs.tests.test_xattr:  testcases for extended attribute support

"""

import unittest
import os

from fs.path import *
from fs.errors import *
from fs.tests import FSTestCases


class XAttrTestCases:
    """Testcases for filesystems providing extended attribute support.

    This class should be used as a mixin to the unittest.TestCase class
    for filesystems that provide extended attribute support.
    """

    def test_getsetdel(self):
        def do_getsetdel(p):
            self.assertEqual(self.fs.getxattr(p,"xattr1"),None)
            self.fs.setxattr(p,"xattr1","value1")
            self.assertEqual(self.fs.getxattr(p,"xattr1"),"value1")
            self.fs.delxattr(p,"xattr1")
            self.assertEqual(self.fs.getxattr(p,"xattr1"),None)
        self.fs.createfile("test.txt","hello")
        do_getsetdel("test.txt")
        self.assertRaises(ResourceNotFoundError,self.fs.getxattr,"test2.txt","xattr1")
        self.fs.makedir("mystuff")
        self.fs.createfile("/mystuff/test.txt","")
        do_getsetdel("mystuff")
        do_getsetdel("mystuff/test.txt")

    def test_list_xattrs(self):
        def do_list(p):
            self.assertEquals(sorted(self.fs.listxattrs(p)),[])
            self.fs.setxattr(p,"xattr1","value1")
            self.assertEquals(self.fs.getxattr(p,"xattr1"),"value1")
            self.assertEquals(sorted(self.fs.listxattrs(p)),["xattr1"])
            self.fs.setxattr(p,"attr2","value2")
            self.assertEquals(sorted(self.fs.listxattrs(p)),["attr2","xattr1"])
            self.fs.delxattr(p,"xattr1")
            self.assertEquals(sorted(self.fs.listxattrs(p)),["attr2"])
            self.fs.delxattr(p,"attr2")
            self.assertEquals(sorted(self.fs.listxattrs(p)),[])
        self.fs.createfile("test.txt","hello")
        do_list("test.txt")
        self.fs.makedir("mystuff")
        self.fs.createfile("/mystuff/test.txt","")
        do_list("mystuff")
        do_list("mystuff/test.txt")

    def test_copy_xattrs(self):
        self.fs.createfile("a.txt","content")
        self.fs.setxattr("a.txt","myattr","myvalue")
        self.fs.setxattr("a.txt","testattr","testvalue")
        self.fs.makedir("stuff")
        self.fs.copy("a.txt","stuff/a.txt")
        self.assertTrue(self.fs.exists("stuff/a.txt"))
        self.assertEquals(self.fs.getxattr("stuff/a.txt","myattr"),"myvalue")
        self.assertEquals(self.fs.getxattr("stuff/a.txt","testattr"),"testvalue")
        self.assertEquals(self.fs.getxattr("a.txt","myattr"),"myvalue")
        self.assertEquals(self.fs.getxattr("a.txt","testattr"),"testvalue")
        self.fs.setxattr("stuff","dirattr","a directory")
        self.fs.copydir("stuff","stuff2")
        self.assertEquals(self.fs.getxattr("stuff2/a.txt","myattr"),"myvalue")
        self.assertEquals(self.fs.getxattr("stuff2/a.txt","testattr"),"testvalue")
        self.assertEquals(self.fs.getxattr("stuff2","dirattr"),"a directory")
        self.assertEquals(self.fs.getxattr("stuff","dirattr"),"a directory")

    def test_move_xattrs(self):
        self.fs.createfile("a.txt","content")
        self.fs.setxattr("a.txt","myattr","myvalue")
        self.fs.setxattr("a.txt","testattr","testvalue")
        self.fs.makedir("stuff")
        self.fs.move("a.txt","stuff/a.txt")
        self.assertTrue(self.fs.exists("stuff/a.txt"))
        self.assertEquals(self.fs.getxattr("stuff/a.txt","myattr"),"myvalue")
        self.assertEquals(self.fs.getxattr("stuff/a.txt","testattr"),"testvalue")
        self.fs.setxattr("stuff","dirattr","a directory")
        self.fs.movedir("stuff","stuff2")
        self.assertEquals(self.fs.getxattr("stuff2/a.txt","myattr"),"myvalue")
        self.assertEquals(self.fs.getxattr("stuff2/a.txt","testattr"),"testvalue")
        self.assertEquals(self.fs.getxattr("stuff2","dirattr"),"a directory")
 


from fs.xattrs import ensure_xattrs

from fs import tempfs
class TestXAttr_TempFS(unittest.TestCase,FSTestCases,XAttrTestCases):

    def setUp(self):
        self.fs = ensure_xattrs(tempfs.TempFS())

    def tearDown(self):
        td = self.fs._temp_dir
        self.fs.close()
        self.assert_(not os.path.exists(td))

    def check(self, p):
        td = self.fs._temp_dir
        return os.path.exists(os.path.join(td, relpath(p)))


from fs import memoryfs
class TestXAttr_MemoryFS(unittest.TestCase,FSTestCases,XAttrTestCases):

    def setUp(self):
        self.fs = ensure_xattrs(memoryfs.MemoryFS())

    def check(self, p):
        return self.fs.exists(p)


