#!/usr/bin/env python
"""

  fs.tests:  testcases for the fs module

"""

#  Send any output from the logging module to stdout, so it will
#  be captured by nose and reported appropriately
import sys
import logging
logging.basicConfig(level=logging.ERROR,stream=sys.stdout)

from fs.base import *

import os, os.path
import pickle


class FSTestCases:
    """Base suite of testcases for filesystem implementations.

    Any FS subclass should be capable of passing all of these tests.
    To apply the tests to your own FS implementation, simply use FSTestCase
    as a mixin for your own unittest.TestCase subclass and have the setUp
    method set self.fs to an instance of your FS implementation.

    This class is designed as a mixin so that it's not detected by test
    loading tools such as nose.
    """

    def check(self, p):
        """Check that a file exists within self.fs"""
        return self.fs.exists(p)

    def test_root_dir(self):
        self.assertTrue(self.fs.isdir(""))
        self.assertTrue(self.fs.isdir("/"))

    def test_debug(self):
        str(self.fs)
        repr(self.fs)
        self.assert_(hasattr(self.fs, 'desc'))

    def test_writefile(self):
        self.assertRaises(ResourceNotFoundError,self.fs.open,"test1.txt")
        f = self.fs.open("test1.txt","w")
        f.write("testing")
        f.close()
        self.check("test1.txt")
        f = self.fs.open("test1.txt","r")
        self.assertEquals(f.read(),"testing")
        f.close()
        f = self.fs.open("test1.txt","w")
        f.write("test file overwrite")
        f.close()
        self.check("test1.txt")
        f = self.fs.open("test1.txt","r")
        self.assertEquals(f.read(),"test file overwrite")

    def test_isdir_isfile(self):
        self.assertFalse(self.fs.exists("dir1"))
        self.assertFalse(self.fs.isdir("dir1"))
        self.assertFalse(self.fs.isfile("a.txt"))
        self.fs.createfile("a.txt")
        self.assertFalse(self.fs.isdir("dir1"))
        self.assertTrue(self.fs.exists("a.txt"))
        self.assertTrue(self.fs.isfile("a.txt"))
        self.fs.makedir("dir1")
        self.assertTrue(self.fs.isdir("dir1"))
        self.assertTrue(self.fs.exists("dir1"))
        self.assertTrue(self.fs.exists("a.txt"))
        self.fs.remove("a.txt")
        self.assertFalse(self.fs.exists("a.txt"))

    def test_listdir(self):
        self.fs.createfile("a")
        self.fs.createfile("b")
        self.fs.createfile("foo")
        self.fs.createfile("bar")
        # Test listing of the root directory
        d1 = self.fs.listdir()
        self.assertEqual(len(d1), 4)
        self.assertEqual(sorted(d1), ["a", "b", "bar", "foo"])
        d1 = self.fs.listdir("")
        self.assertEqual(len(d1), 4)
        self.assertEqual(sorted(d1), ["a", "b", "bar", "foo"])
        d1 = self.fs.listdir("/")
        self.assertEqual(len(d1), 4)
        # Test listing absolute paths
        d2 = self.fs.listdir(absolute=True)
        self.assertEqual(len(d2), 4)
        self.assertEqual(sorted(d2), ["/a", "/b", "/bar", "/foo"])
        # Create some deeper subdirectories, to make sure their
        # contents are not inadvertantly included
        self.fs.makedir("p/1/2/3",recursive=True)
        self.fs.createfile("p/1/2/3/a")
        self.fs.createfile("p/1/2/3/b")
        self.fs.createfile("p/1/2/3/foo")
        self.fs.createfile("p/1/2/3/bar")
        self.fs.makedir("q")
        # Test listing just files, just dirs, and wildcards
        dirs_only = self.fs.listdir(dirs_only=True)
        files_only = self.fs.listdir(files_only=True)
        contains_a = self.fs.listdir(wildcard="*a*")
        self.assertEqual(sorted(dirs_only), ["p", "q"])
        self.assertEqual(sorted(files_only), ["a", "b", "bar", "foo"])
        self.assertEqual(sorted(contains_a), ["a", "bar"])
        # Test listing a subdirectory
        d3 = self.fs.listdir("p/1/2/3")
        self.assertEqual(len(d3), 4)
        self.assertEqual(sorted(d3), ["a", "b", "bar", "foo"])
        # Test listing a subdirectory with absoliute and full paths
        d4 = self.fs.listdir("p/1/2/3", absolute=True)
        self.assertEqual(len(d4), 4)
        self.assertEqual(sorted(d4), ["/p/1/2/3/a", "/p/1/2/3/b", "/p/1/2/3/bar", "/p/1/2/3/foo"])
        d4 = self.fs.listdir("p/1/2/3", full=True)
        self.assertEqual(len(d4), 4)
        self.assertEqual(sorted(d4), ["p/1/2/3/a", "p/1/2/3/b", "p/1/2/3/bar", "p/1/2/3/foo"])
        # Test that appropriate errors are raised
        self.assertRaises(ResourceNotFoundError,self.fs.listdir,"zebra")
        self.assertRaises(ResourceInvalidError,self.fs.listdir,"foo")
        
    def test_makedir(self):
        check = self.check
        self.fs.makedir("a")
        self.assertTrue(check("a"))
        self.assertRaises(ParentDirectoryMissingError,self.fs.makedir,"a/b/c")
        self.fs.makedir("a/b/c", recursive=True)
        self.assert_(check("a/b/c"))
        self.fs.makedir("foo/bar/baz", recursive=True)
        self.assert_(check("foo/bar/baz"))
        self.fs.makedir("a/b/child")
        self.assert_(check("a/b/child"))
        self.assertRaises(DestinationExistsError,self.fs.makedir,"/a/b")
        self.fs.makedir("/a/b",allow_recreate=True)
        self.fs.createfile("/a/file")
        self.assertRaises(ResourceInvalidError,self.fs.makedir,"a/file")

    def test_remove(self):
        self.fs.createfile("a.txt")
        self.assertTrue(self.check("a.txt"))
        self.fs.remove("a.txt")
        self.assertFalse(self.check("a.txt"))
        self.assertRaises(ResourceNotFoundError,self.fs.remove,"a.txt")
        self.fs.makedir("dir1")
        self.assertRaises(ResourceInvalidError,self.fs.remove,"dir1")
        self.fs.createfile("/dir1/a.txt")
        self.assertTrue(self.check("dir1/a.txt"))
        self.fs.remove("dir1/a.txt")
        self.assertFalse(self.check("/dir1/a.txt"))

    def test_removedir(self):
        check = self.check
        self.fs.makedir("a")
        self.assert_(check("a"))
        self.fs.removedir("a")
        self.assert_(not check("a"))
        self.fs.makedir("a/b/c/d", recursive=True)
        self.assertRaises(DirectoryNotEmptyError, self.fs.removedir, "a/b")
        self.fs.removedir("a/b/c/d")
        self.assert_(not check("a/b/c/d"))
        self.fs.removedir("a/b/c")
        self.assert_(not check("a/b/c"))
        self.fs.removedir("a/b")
        self.assert_(not check("a/b"))
        #  Test recursive removal of empty parent dirs
        self.fs.makedir("foo/bar/baz", recursive=True)
        self.fs.removedir("foo/bar/baz", recursive=True)
        self.assert_(not check("foo/bar/baz"))
        self.assert_(not check("foo/bar"))
        self.assert_(not check("foo"))
        #  Ensure that force=True works as expected
        self.fs.makedir("frollic/waggle", recursive=True)
        self.fs.createfile("frollic/waddle.txt","waddlewaddlewaddle")
        self.assertRaises(DirectoryNotEmptyError,self.fs.removedir,"frollic")
        self.assertRaises(ResourceInvalidError,self.fs.removedir,"frollic/waddle.txt")
        self.fs.removedir("frollic",force=True)
        self.assert_(not check("frollic"))

    def test_rename(self):
        check = self.check
        self.fs.createfile("foo.txt","Hello, World!")
        self.assert_(check("foo.txt"))
        self.fs.rename("foo.txt", "bar.txt")
        self.assert_(check("bar.txt"))
        self.assert_(not check("foo.txt"))

    def test_info(self):
        test_str = "Hello, World!"
        self.fs.createfile("info.txt",test_str)
        info = self.fs.getinfo("info.txt")
        self.assertEqual(info['size'], len(test_str))
        self.fs.desc("info.txt")

    def test_getsize(self):
        test_str = "*"*23
        self.fs.createfile("info.txt",test_str)
        size = self.fs.getsize("info.txt")
        self.assertEqual(size, len(test_str))

    def test_movefile(self):
        check = self.check
        contents = "If the implementation is hard to explain, it's a bad idea."
        def makefile(path):
            self.fs.createfile(path,contents)
        def checkcontents(path):
            check_contents = self.fs.getcontents(path)
            self.assertEqual(check_contents,contents)
            return contents == check_contents

        self.fs.makedir("foo/bar", recursive=True)
        makefile("foo/bar/a.txt")
        self.assert_(check("foo/bar/a.txt"))
        self.assert_(checkcontents("foo/bar/a.txt"))
        self.fs.move("foo/bar/a.txt", "foo/b.txt")
        self.assert_(not check("foo/bar/a.txt"))
        self.assert_(check("foo/b.txt"))
        self.assert_(checkcontents("foo/b.txt"))

        self.fs.move("foo/b.txt", "c.txt")
        self.assert_(not check("foo/b.txt"))
        self.assert_(check("/c.txt"))
        self.assert_(checkcontents("/c.txt"))

        makefile("foo/bar/a.txt")
        self.assertRaises(DestinationExistsError,self.fs.move,"foo/bar/a.txt","/c.txt")
        self.assert_(check("foo/bar/a.txt"))
        self.assert_(check("/c.txt"))
        self.fs.move("foo/bar/a.txt","/c.txt",overwrite=True)
        self.assert_(not check("foo/bar/a.txt"))
        self.assert_(check("/c.txt"))


    def test_movedir(self):
        check = self.check
        contents = "If the implementation is hard to explain, it's a bad idea."
        def makefile(path):
            self.fs.createfile(path,contents)

        self.fs.makedir("a")
        self.fs.makedir("b")
        makefile("a/1.txt")
        makefile("a/2.txt")
        makefile("a/3.txt")
        self.fs.makedir("a/foo/bar", recursive=True)
        makefile("a/foo/bar/baz.txt")

        self.fs.movedir("a", "copy of a")

        self.assert_(check("copy of a/1.txt"))
        self.assert_(check("copy of a/2.txt"))
        self.assert_(check("copy of a/3.txt"))
        self.assert_(check("copy of a/foo/bar/baz.txt"))

        self.assert_(not check("a/1.txt"))
        self.assert_(not check("a/2.txt"))
        self.assert_(not check("a/3.txt"))
        self.assert_(not check("a/foo/bar/baz.txt"))
        self.assert_(not check("a/foo/bar"))
        self.assert_(not check("a/foo"))
        self.assert_(not check("a"))

        self.fs.makedir("a")
        self.assertRaises(DestinationExistsError,self.fs.movedir,"copy of a","a")
        self.fs.movedir("copy of a","a",overwrite=True)
        self.assert_(not check("copy of a"))
        self.assert_(check("a/1.txt"))
        self.assert_(check("a/2.txt"))
        self.assert_(check("a/3.txt"))
        self.assert_(check("a/foo/bar/baz.txt"))


    def test_copyfile(self):
        check = self.check
        contents = "If the implementation is hard to explain, it's a bad idea."
        def makefile(path,contents=contents):
            self.fs.createfile(path,contents)
        def checkcontents(path,contents=contents):
            check_contents = self.fs.getcontents(path)
            self.assertEqual(check_contents,contents)
            return contents == check_contents

        self.fs.makedir("foo/bar", recursive=True)
        makefile("foo/bar/a.txt")
        self.assert_(check("foo/bar/a.txt"))
        self.assert_(checkcontents("foo/bar/a.txt"))
        self.fs.copy("foo/bar/a.txt", "foo/b.txt")
        self.assert_(check("foo/bar/a.txt"))
        self.assert_(check("foo/b.txt"))
        self.assert_(checkcontents("foo/b.txt"))

        self.fs.copy("foo/b.txt", "c.txt")
        self.assert_(check("foo/b.txt"))
        self.assert_(check("/c.txt"))
        self.assert_(checkcontents("/c.txt"))

        makefile("foo/bar/a.txt","different contents")
        self.assertRaises(DestinationExistsError,self.fs.copy,"foo/bar/a.txt","/c.txt")
        self.assert_(checkcontents("/c.txt"))
        self.fs.copy("foo/bar/a.txt","/c.txt",overwrite=True)
        self.assert_(checkcontents("foo/bar/a.txt","different contents"))
        self.assert_(checkcontents("/c.txt","different contents"))


    def test_copydir(self):
        check = self.check
        contents = "If the implementation is hard to explain, it's a bad idea."
        def makefile(path):
            self.fs.createfile(path,contents)
        def checkcontents(path):
            check_contents = self.fs.getcontents(path)
            self.assertEqual(check_contents,contents)
            return contents == check_contents

        self.fs.makedir("a")
        self.fs.makedir("b")
        makefile("a/1.txt")
        makefile("a/2.txt")
        makefile("a/3.txt")
        self.fs.makedir("a/foo/bar", recursive=True)
        makefile("a/foo/bar/baz.txt")

        self.fs.copydir("a", "copy of a")
        self.assert_(check("copy of a/1.txt"))
        self.assert_(check("copy of a/2.txt"))
        self.assert_(check("copy of a/3.txt"))
        self.assert_(check("copy of a/foo/bar/baz.txt"))
        checkcontents("copy of a/1.txt")

        self.assert_(check("a/1.txt"))
        self.assert_(check("a/2.txt"))
        self.assert_(check("a/3.txt"))
        self.assert_(check("a/foo/bar/baz.txt"))
        checkcontents("a/1.txt")

        self.assertRaises(DestinationExistsError,self.fs.copydir,"a","b")
        self.fs.copydir("a","b",overwrite=True)
        self.assert_(check("b/1.txt"))
        self.assert_(check("b/2.txt"))
        self.assert_(check("b/3.txt"))
        self.assert_(check("b/foo/bar/baz.txt"))
        checkcontents("b/1.txt")

    def test_copydir_with_dotfile(self):
        check = self.check
        contents = "If the implementation is hard to explain, it's a bad idea."
        def makefile(path):
            self.fs.createfile(path,contents)

        self.fs.makedir("a")
        makefile("a/1.txt")
        makefile("a/2.txt")
        makefile("a/.hidden.txt")

        self.fs.copydir("a", "copy of a")
        self.assert_(check("copy of a/1.txt"))
        self.assert_(check("copy of a/2.txt"))
        self.assert_(check("copy of a/.hidden.txt"))

        self.assert_(check("a/1.txt"))
        self.assert_(check("a/2.txt"))
        self.assert_(check("a/.hidden.txt"))

    def test_readwriteappendseek(self):
        def checkcontents(path, check_contents):
            read_contents = self.fs.getcontents(path)
            self.assertEqual(read_contents,check_contents)
            return read_contents == check_contents
        test_strings = ["Beautiful is better than ugly.",
                        "Explicit is better than implicit.",
                        "Simple is better than complex."]
        all_strings = "".join(test_strings)

        self.assertRaises(ResourceNotFoundError, self.fs.open, "a.txt", "r")
        self.assert_(not self.fs.exists("a.txt"))
        f1 = self.fs.open("a.txt", "wb")
        pos = 0
        for s in test_strings:
            f1.write(s)
            pos += len(s)
            self.assertEqual(pos, f1.tell())
        f1.close()
        self.assert_(self.fs.exists("a.txt"))
        self.assert_(checkcontents("a.txt", all_strings))

        f2 = self.fs.open("b.txt", "wb")
        f2.write(test_strings[0])
        f2.close()
        self.assert_(checkcontents("b.txt", test_strings[0]))
        f3 = self.fs.open("b.txt", "ab")
        f3.write(test_strings[1])
        f3.write(test_strings[2])
        f3.close()
        self.assert_(checkcontents("b.txt", all_strings))
        f4 = self.fs.open("b.txt", "wb")
        f4.write(test_strings[2])
        f4.close()
        self.assert_(checkcontents("b.txt", test_strings[2]))
        f5 = self.fs.open("c.txt", "wb")
        for s in test_strings:
            f5.write(s+"\n")
        f5.close()
        f6 = self.fs.open("c.txt", "rb")
        for s, t in zip(f6, test_strings):
            self.assertEqual(s, t+"\n")
        f6.close()
        f7 = self.fs.open("c.txt", "rb")
        f7.seek(13)
        word = f7.read(6)
        self.assertEqual(word, "better")
        f7.seek(1, os.SEEK_CUR)
        word = f7.read(4)
        self.assertEqual(word, "than")
        f7.seek(-9, os.SEEK_END)
        word = f7.read(7)
        self.assertEqual(word, "complex")
        f7.close()
        self.assertEqual(self.fs.getcontents("a.txt"), all_strings)

    def test_with_statement(self):
        #  This is a little tricky since 'with' is actually new syntax.
        #  We use eval() to make this method safe for old python versions.
        import sys
        if sys.version_info[0] >= 2 and sys.version_info[1] >= 5:
            #  A successful 'with' statement
            contents = "testing the with statement"
            code = "from __future__ import with_statement\n"
            code += "with self.fs.open('f.txt','w-') as testfile:\n"
            code += "    testfile.write(contents)\n"
            code += "self.assertEquals(self.fs.getcontents('f.txt'),contents)"
            code = compile(code,"<string>",'exec')
            eval(code)
            # A 'with' statement raising an error
            contents = "testing the with statement"
            code = "from __future__ import with_statement\n"
            code += "with self.fs.open('f.txt','w-') as testfile:\n"
            code += "    testfile.write(contents)\n"
            code += "    raise ValueError\n"
            code = compile(code,"<string>",'exec')
            self.assertRaises(ValueError,eval,code,globals(),locals())
            self.assertEquals(self.fs.getcontents('f.txt'),contents)

    def test_pickling(self):
        self.fs.createfile("test1","hello world")
        fs2 = pickle.loads(pickle.dumps(self.fs))
        self.assert_(fs2.isfile("test1"))

