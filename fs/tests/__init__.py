#!/usr/bin/env python
"""

  fs.tests:  testcases for the fs module

"""

from __future__ import with_statement

#  Send any output from the logging module to stdout, so it will
#  be captured by nose and reported appropriately
import sys
import logging
logging.basicConfig(level=logging.ERROR, stream=sys.stdout)

from fs.base import *
from fs.path import *
from fs.errors import *
from fs.filelike import StringIO

import datetime
import unittest
import os
import os.path
import pickle
import random
import copy

import time
try:
    import threading
except ImportError:
    import dummy_threading as threading

import six
from six import PY3, b


class FSTestCases(object):
    """Base suite of testcases for filesystem implementations.

    Any FS subclass should be capable of passing all of these tests.
    To apply the tests to your own FS implementation, simply use FSTestCase
    as a mixin for your own unittest.TestCase subclass and have the setUp
    method set self.fs to an instance of your FS implementation.

    NB. The Filesystem being tested must have a capacity of at least 3MB.

    This class is designed as a mixin so that it's not detected by test
    loading tools such as nose.
    """

    def check(self, p):
        """Check that a file exists within self.fs"""
        return self.fs.exists(p)

    def test_invalid_chars(self):
        """Check paths validate ok"""
        # Will have to be overriden selectively for custom validepath methods
        self.assertEqual(self.fs.validatepath(''), None)
        self.assertEqual(self.fs.validatepath('.foo'), None)
        self.assertEqual(self.fs.validatepath('foo'), None)
        self.assertEqual(self.fs.validatepath('foo/bar'), None)
        self.assert_(self.fs.isvalidpath('foo/bar'))

    def test_tree(self):
        """Test tree print"""
        self.fs.makedir('foo')
        self.fs.createfile('foo/bar.txt')
        self.fs.tree()

    def test_meta(self):
        """Checks getmeta / hasmeta are functioning"""
        # getmeta / hasmeta are hard to test, since there is no way to validate
        # the implementation's response
        meta_names = ["read_only",
                      "network",
                      "unicode_paths"]
        stupid_meta = 'thismetashouldnotexist!"r$$%^&&*()_+'
        self.assertRaises(NoMetaError, self.fs.getmeta, stupid_meta)
        self.assertFalse(self.fs.hasmeta(stupid_meta))
        self.assertEquals(None, self.fs.getmeta(stupid_meta, None))
        self.assertEquals(3.14, self.fs.getmeta(stupid_meta, 3.14))
        for meta_name in meta_names:
            try:
                meta = self.fs.getmeta(meta_name)
                self.assertTrue(self.fs.hasmeta(meta_name))
            except NoMetaError:
                self.assertFalse(self.fs.hasmeta(meta_name))

    def test_root_dir(self):
        self.assertTrue(self.fs.isdir(""))
        self.assertTrue(self.fs.isdir("/"))
        # These may be false (e.g. empty dict) but mustn't raise errors
        self.fs.getinfo("")
        self.assertTrue(self.fs.getinfo("/") is not None)

    def test_getsyspath(self):
        try:
            syspath = self.fs.getsyspath("/")
        except NoSysPathError:
            pass
        else:
            self.assertTrue(isinstance(syspath, unicode))
        syspath = self.fs.getsyspath("/", allow_none=True)
        if syspath is not None:
            self.assertTrue(isinstance(syspath, unicode))

    def test_debug(self):
        str(self.fs)
        repr(self.fs)
        self.assert_(hasattr(self.fs, 'desc'))

    def test_open_on_directory(self):
        self.fs.makedir("testdir")
        try:
            f = self.fs.open("testdir")
        except ResourceInvalidError:
            pass
        except Exception:
            raise
            ecls = sys.exc_info()[0]
            assert False, "%s raised instead of ResourceInvalidError" % (ecls,)
        else:
            f.close()
            assert False, "ResourceInvalidError was not raised"

    def test_writefile(self):
        self.assertRaises(ResourceNotFoundError, self.fs.open, "test1.txt")
        f = self.fs.open("test1.txt", "wb")
        f.write(b("testing"))
        f.close()
        self.assertTrue(self.check("test1.txt"))
        f = self.fs.open("test1.txt", "rb")
        self.assertEquals(f.read(), b("testing"))
        f.close()
        f = self.fs.open("test1.txt", "wb")
        f.write(b("test file overwrite"))
        f.close()
        self.assertTrue(self.check("test1.txt"))
        f = self.fs.open("test1.txt", "rb")
        self.assertEquals(f.read(), b("test file overwrite"))
        f.close()

    def test_createfile(self):
        test = b('now with content')
        self.fs.createfile("test.txt")
        self.assert_(self.fs.exists("test.txt"))
        self.assertEqual(self.fs.getcontents("test.txt", "rb"), b(''))
        self.fs.setcontents("test.txt", test)
        self.fs.createfile("test.txt")
        self.assertEqual(self.fs.getcontents("test.txt", "rb"), test)
        self.fs.createfile("test.txt", wipe=True)
        self.assertEqual(self.fs.getcontents("test.txt", "rb"), b(''))

    def test_readline(self):
        text = b"Hello\nWorld\n"
        self.fs.setcontents('a.txt', text)
        with self.fs.open('a.txt', 'rb') as f:
            line = f.readline()
        self.assertEqual(line, b"Hello\n")

    def test_setcontents(self):
        #  setcontents() should accept both a string...
        self.fs.setcontents("hello", b("world"))
        self.assertEquals(self.fs.getcontents("hello", "rb"), b("world"))
        #  ...and a file-like object
        self.fs.setcontents("hello", StringIO(b("to you, good sir!")))
        self.assertEquals(self.fs.getcontents(
            "hello", "rb"), b("to you, good sir!"))
        #  setcontents() should accept both a string...
        self.fs.setcontents("hello", b("world"), chunk_size=2)
        self.assertEquals(self.fs.getcontents("hello", "rb"), b("world"))
        #  ...and a file-like object
        self.fs.setcontents("hello", StringIO(
            b("to you, good sir!")), chunk_size=2)
        self.assertEquals(self.fs.getcontents(
            "hello", "rb"), b("to you, good sir!"))
        self.fs.setcontents("hello", b(""))
        self.assertEquals(self.fs.getcontents("hello", "rb"), b(""))

    def test_setcontents_async(self):
        #  setcontents() should accept both a string...
        self.fs.setcontents_async("hello", b("world")).wait()
        self.assertEquals(self.fs.getcontents("hello", "rb"), b("world"))
        #  ...and a file-like object
        self.fs.setcontents_async("hello", StringIO(
            b("to you, good sir!"))).wait()
        self.assertEquals(self.fs.getcontents("hello"), b("to you, good sir!"))
        self.fs.setcontents_async("hello", b("world"), chunk_size=2).wait()
        self.assertEquals(self.fs.getcontents("hello", "rb"), b("world"))
        #  ...and a file-like object
        self.fs.setcontents_async("hello", StringIO(
            b("to you, good sir!")), chunk_size=2).wait()
        self.assertEquals(self.fs.getcontents(
            "hello", "rb"), b("to you, good sir!"))

    def test_isdir_isfile(self):
        self.assertFalse(self.fs.exists("dir1"))
        self.assertFalse(self.fs.isdir("dir1"))
        self.assertFalse(self.fs.isfile("a.txt"))
        self.fs.setcontents("a.txt", b(''))
        self.assertFalse(self.fs.isdir("dir1"))
        self.assertTrue(self.fs.exists("a.txt"))
        self.assertTrue(self.fs.isfile("a.txt"))
        self.assertFalse(self.fs.exists("a.txt/thatsnotadir"))
        self.fs.makedir("dir1")
        self.assertTrue(self.fs.isdir("dir1"))
        self.assertTrue(self.fs.exists("dir1"))
        self.assertTrue(self.fs.exists("a.txt"))
        self.fs.remove("a.txt")
        self.assertFalse(self.fs.exists("a.txt"))

    def test_listdir(self):
        def check_unicode(items):
            for item in items:
                self.assertTrue(isinstance(item, unicode))
        self.fs.setcontents(u"a", b(''))
        self.fs.setcontents("b", b(''))
        self.fs.setcontents("foo", b(''))
        self.fs.setcontents("bar", b(''))
        # Test listing of the root directory
        d1 = self.fs.listdir()
        self.assertEqual(len(d1), 4)
        self.assertEqual(sorted(d1), [u"a", u"b", u"bar", u"foo"])
        check_unicode(d1)
        d1 = self.fs.listdir("")
        self.assertEqual(len(d1), 4)
        self.assertEqual(sorted(d1), [u"a", u"b", u"bar", u"foo"])
        check_unicode(d1)
        d1 = self.fs.listdir("/")
        self.assertEqual(len(d1), 4)
        check_unicode(d1)
        # Test listing absolute paths
        d2 = self.fs.listdir(absolute=True)
        self.assertEqual(len(d2), 4)
        self.assertEqual(sorted(d2), [u"/a", u"/b", u"/bar", u"/foo"])
        check_unicode(d2)
        # Create some deeper subdirectories, to make sure their
        # contents are not inadvertantly included
        self.fs.makedir("p/1/2/3", recursive=True)
        self.fs.setcontents("p/1/2/3/a", b(''))
        self.fs.setcontents("p/1/2/3/b", b(''))
        self.fs.setcontents("p/1/2/3/foo", b(''))
        self.fs.setcontents("p/1/2/3/bar", b(''))
        self.fs.makedir("q")
        # Test listing just files, just dirs, and wildcards
        dirs_only = self.fs.listdir(dirs_only=True)
        files_only = self.fs.listdir(files_only=True)
        contains_a = self.fs.listdir(wildcard="*a*")
        self.assertEqual(sorted(dirs_only), [u"p", u"q"])
        self.assertEqual(sorted(files_only), [u"a", u"b", u"bar", u"foo"])
        self.assertEqual(sorted(contains_a), [u"a", u"bar"])
        check_unicode(dirs_only)
        check_unicode(files_only)
        check_unicode(contains_a)
        # Test listing a subdirectory
        d3 = self.fs.listdir("p/1/2/3")
        self.assertEqual(len(d3), 4)
        self.assertEqual(sorted(d3), [u"a", u"b", u"bar", u"foo"])
        check_unicode(d3)
        # Test listing a subdirectory with absoliute and full paths
        d4 = self.fs.listdir("p/1/2/3", absolute=True)
        self.assertEqual(len(d4), 4)
        self.assertEqual(sorted(d4), [u"/p/1/2/3/a", u"/p/1/2/3/b", u"/p/1/2/3/bar", u"/p/1/2/3/foo"])
        check_unicode(d4)
        d4 = self.fs.listdir("p/1/2/3", full=True)
        self.assertEqual(len(d4), 4)
        self.assertEqual(sorted(d4), [u"p/1/2/3/a", u"p/1/2/3/b", u"p/1/2/3/bar", u"p/1/2/3/foo"])
        check_unicode(d4)
        # Test that appropriate errors are raised
        self.assertRaises(ResourceNotFoundError, self.fs.listdir, "zebra")
        self.assertRaises(ResourceInvalidError, self.fs.listdir, "foo")

    def test_listdirinfo(self):
        def check_unicode(items):
            for (nm, info) in items:
                self.assertTrue(isinstance(nm, unicode))

        def check_equal(items, target):
            names = [nm for (nm, info) in items]
            self.assertEqual(sorted(names), sorted(target))
        self.fs.setcontents(u"a", b(''))
        self.fs.setcontents("b", b(''))
        self.fs.setcontents("foo", b(''))
        self.fs.setcontents("bar", b(''))
        # Test listing of the root directory
        d1 = self.fs.listdirinfo()
        self.assertEqual(len(d1), 4)
        check_equal(d1, [u"a", u"b", u"bar", u"foo"])
        check_unicode(d1)
        d1 = self.fs.listdirinfo("")
        self.assertEqual(len(d1), 4)
        check_equal(d1, [u"a", u"b", u"bar", u"foo"])
        check_unicode(d1)
        d1 = self.fs.listdirinfo("/")
        self.assertEqual(len(d1), 4)
        check_equal(d1, [u"a", u"b", u"bar", u"foo"])
        check_unicode(d1)
        # Test listing absolute paths
        d2 = self.fs.listdirinfo(absolute=True)
        self.assertEqual(len(d2), 4)
        check_equal(d2, [u"/a", u"/b", u"/bar", u"/foo"])
        check_unicode(d2)
        # Create some deeper subdirectories, to make sure their
        # contents are not inadvertantly included
        self.fs.makedir("p/1/2/3", recursive=True)
        self.fs.setcontents("p/1/2/3/a", b(''))
        self.fs.setcontents("p/1/2/3/b", b(''))
        self.fs.setcontents("p/1/2/3/foo", b(''))
        self.fs.setcontents("p/1/2/3/bar", b(''))
        self.fs.makedir("q")
        # Test listing just files, just dirs, and wildcards
        dirs_only = self.fs.listdirinfo(dirs_only=True)
        files_only = self.fs.listdirinfo(files_only=True)
        contains_a = self.fs.listdirinfo(wildcard="*a*")
        check_equal(dirs_only, [u"p", u"q"])
        check_equal(files_only, [u"a", u"b", u"bar", u"foo"])
        check_equal(contains_a, [u"a", u"bar"])
        check_unicode(dirs_only)
        check_unicode(files_only)
        check_unicode(contains_a)
        # Test listing a subdirectory
        d3 = self.fs.listdirinfo("p/1/2/3")
        self.assertEqual(len(d3), 4)
        check_equal(d3, [u"a", u"b", u"bar", u"foo"])
        check_unicode(d3)
        # Test listing a subdirectory with absoliute and full paths
        d4 = self.fs.listdirinfo("p/1/2/3", absolute=True)
        self.assertEqual(len(d4), 4)
        check_equal(d4, [u"/p/1/2/3/a", u"/p/1/2/3/b", u"/p/1/2/3/bar", u"/p/1/2/3/foo"])
        check_unicode(d4)
        d4 = self.fs.listdirinfo("p/1/2/3", full=True)
        self.assertEqual(len(d4), 4)
        check_equal(d4, [u"p/1/2/3/a", u"p/1/2/3/b", u"p/1/2/3/bar", u"p/1/2/3/foo"])
        check_unicode(d4)
        # Test that appropriate errors are raised
        self.assertRaises(ResourceNotFoundError, self.fs.listdirinfo, "zebra")
        self.assertRaises(ResourceInvalidError, self.fs.listdirinfo, "foo")

    def test_walk(self):
        self.fs.setcontents('a.txt', b('hello'))
        self.fs.setcontents('b.txt', b('world'))
        self.fs.makeopendir('foo').setcontents('c', b('123'))
        sorted_walk = sorted([(d, sorted(fs)) for (d, fs) in self.fs.walk()])
        self.assertEquals(sorted_walk,
                          [("/", ["a.txt", "b.txt"]),
                           ("/foo", ["c"])])
        #  When searching breadth-first, shallow entries come first
        found_a = False
        for _, files in self.fs.walk(search="breadth"):
            if "a.txt" in files:
                found_a = True
            if "c" in files:
                break
        assert found_a, "breadth search order was wrong"
        #  When searching depth-first, deep entries come first
        found_c = False
        for _, files in self.fs.walk(search="depth"):
            if "c" in files:
                found_c = True
            if "a.txt" in files:
                break
        assert found_c, "depth search order was wrong: " + \
            str(list(self.fs.walk(search="depth")))

    def test_walk_wildcard(self):
        self.fs.setcontents('a.txt', b('hello'))
        self.fs.setcontents('b.txt', b('world'))
        self.fs.makeopendir('foo').setcontents('c', b('123'))
        self.fs.makeopendir('.svn').setcontents('ignored', b(''))
        for dir_path, paths in self.fs.walk(wildcard='*.txt'):
            for path in paths:
                self.assert_(path.endswith('.txt'))
        for dir_path, paths in self.fs.walk(wildcard=lambda fn: fn.endswith('.txt')):
            for path in paths:
                self.assert_(path.endswith('.txt'))

    def test_walk_dir_wildcard(self):
        self.fs.setcontents('a.txt', b('hello'))
        self.fs.setcontents('b.txt', b('world'))
        self.fs.makeopendir('foo').setcontents('c', b('123'))
        self.fs.makeopendir('.svn').setcontents('ignored', b(''))
        for dir_path, paths in self.fs.walk(dir_wildcard=lambda fn: not fn.endswith('.svn')):
            for path in paths:
                self.assert_('.svn' not in path)

    def test_walkfiles(self):
        self.fs.makeopendir('bar').setcontents('a.txt', b('123'))
        self.fs.makeopendir('foo').setcontents('b', b('123'))
        self.assertEquals(sorted(
            self.fs.walkfiles()), ["/bar/a.txt", "/foo/b"])
        self.assertEquals(sorted(self.fs.walkfiles(
            dir_wildcard="*foo*")), ["/foo/b"])
        self.assertEquals(sorted(self.fs.walkfiles(
            wildcard="*.txt")), ["/bar/a.txt"])

    def test_walkdirs(self):
        self.fs.makeopendir('bar').setcontents('a.txt', b('123'))
        self.fs.makeopendir('foo').makeopendir(
            "baz").setcontents('b', b('123'))
        self.assertEquals(sorted(self.fs.walkdirs()), [
                          "/", "/bar", "/foo", "/foo/baz"])
        self.assertEquals(sorted(self.fs.walkdirs(
            wildcard="*foo*")), ["/", "/foo", "/foo/baz"])

    def test_unicode(self):
        alpha = u"\N{GREEK SMALL LETTER ALPHA}"
        beta = u"\N{GREEK SMALL LETTER BETA}"
        self.fs.makedir(alpha)
        self.fs.setcontents(alpha + "/a", b(''))
        self.fs.setcontents(alpha + "/" + beta, b(''))
        self.assertTrue(self.check(alpha))
        self.assertEquals(sorted(self.fs.listdir(alpha)), ["a", beta])

    def test_makedir(self):
        check = self.check
        self.fs.makedir("a")
        self.assertTrue(check("a"))
        self.assertRaises(
            ParentDirectoryMissingError, self.fs.makedir, "a/b/c")
        self.fs.makedir("a/b/c", recursive=True)
        self.assert_(check("a/b/c"))
        self.fs.makedir("foo/bar/baz", recursive=True)
        self.assert_(check("foo/bar/baz"))
        self.fs.makedir("a/b/child")
        self.assert_(check("a/b/child"))
        self.assertRaises(DestinationExistsError, self.fs.makedir, "/a/b")
        self.fs.makedir("/a/b", allow_recreate=True)
        self.fs.setcontents("/a/file", b(''))
        self.assertRaises(ResourceInvalidError, self.fs.makedir, "a/file")

    def test_remove(self):
        self.fs.setcontents("a.txt", b(''))
        self.assertTrue(self.check("a.txt"))
        self.fs.remove("a.txt")
        self.assertFalse(self.check("a.txt"))
        self.assertRaises(ResourceNotFoundError, self.fs.remove, "a.txt")
        self.fs.makedir("dir1")
        self.assertRaises(ResourceInvalidError, self.fs.remove, "dir1")
        self.fs.setcontents("/dir1/a.txt", b(''))
        self.assertTrue(self.check("dir1/a.txt"))
        self.fs.remove("dir1/a.txt")
        self.assertFalse(self.check("/dir1/a.txt"))

    def test_removedir(self):
        check = self.check
        self.fs.makedir("a")
        self.assert_(check("a"))
        self.fs.removedir("a")
        self.assertRaises(ResourceNotFoundError, self.fs.removedir, "a")
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
        self.fs.makedir("foo/bar/baz", recursive=True)
        self.fs.setcontents("foo/file.txt", b("please don't delete me"))
        self.fs.removedir("foo/bar/baz", recursive=True)
        self.assert_(not check("foo/bar/baz"))
        self.assert_(not check("foo/bar"))
        self.assert_(check("foo/file.txt"))
        #  Ensure that force=True works as expected
        self.fs.makedir("frollic/waggle", recursive=True)
        self.fs.setcontents("frollic/waddle.txt", b("waddlewaddlewaddle"))
        self.assertRaises(DirectoryNotEmptyError, self.fs.removedir, "frollic")
        self.assertRaises(
            ResourceInvalidError, self.fs.removedir, "frollic/waddle.txt")
        self.fs.removedir("frollic", force=True)
        self.assert_(not check("frollic"))
        #  Test removing unicode dirs
        kappa = u"\N{GREEK CAPITAL LETTER KAPPA}"
        self.fs.makedir(kappa)
        self.assert_(self.fs.isdir(kappa))
        self.fs.removedir(kappa)
        self.assertRaises(ResourceNotFoundError, self.fs.removedir, kappa)
        self.assert_(not self.fs.isdir(kappa))
        self.fs.makedir(pathjoin("test", kappa), recursive=True)
        self.assert_(check(pathjoin("test", kappa)))
        self.fs.removedir("test", force=True)
        self.assert_(not check("test"))

    def test_rename(self):
        check = self.check
        # test renaming a file in the same directory
        self.fs.setcontents("foo.txt", b("Hello, World!"))
        self.assert_(check("foo.txt"))
        self.fs.rename("foo.txt", "bar.txt")
        self.assert_(check("bar.txt"))
        self.assert_(not check("foo.txt"))
        # test renaming a directory in the same directory
        self.fs.makedir("dir_a")
        self.fs.setcontents("dir_a/test.txt", b("testerific"))
        self.assert_(check("dir_a"))
        self.fs.rename("dir_a", "dir_b")
        self.assert_(check("dir_b"))
        self.assert_(check("dir_b/test.txt"))
        self.assert_(not check("dir_a/test.txt"))
        self.assert_(not check("dir_a"))
        # test renaming a file into a different directory
        self.fs.makedir("dir_a")
        self.fs.rename("dir_b/test.txt", "dir_a/test.txt")
        self.assert_(not check("dir_b/test.txt"))
        self.assert_(check("dir_a/test.txt"))
        # test renaming a file into a non-existent  directory
        self.assertRaises(ParentDirectoryMissingError,
                          self.fs.rename, "dir_a/test.txt", "nonexistent/test.txt")

    def test_info(self):
        test_str = b("Hello, World!")
        self.fs.setcontents("info.txt", test_str)
        info = self.fs.getinfo("info.txt")
        self.assertEqual(info['size'], len(test_str))
        self.fs.desc("info.txt")
        self.assertRaises(ResourceNotFoundError, self.fs.getinfo, "notafile")
        self.assertRaises(
            ResourceNotFoundError, self.fs.getinfo, "info.txt/inval")

    def test_infokeys(self):
        test_str = b("Hello, World!")
        self.fs.setcontents("info.txt", test_str)
        info = self.fs.getinfo("info.txt")
        for k, v in info.iteritems():
            self.assertEqual(self.fs.getinfokeys('info.txt', k), {k: v})

        test_info = {}
        if 'modified_time' in info:
            test_info['modified_time'] = info['modified_time']
        if 'size' in info:
            test_info['size'] = info['size']
        self.assertEqual(self.fs.getinfokeys('info.txt', 'size', 'modified_time'), test_info)
        self.assertEqual(self.fs.getinfokeys('info.txt', 'thiscantpossiblyexistininfo'), {})

    def test_getsize(self):
        test_str = b("*") * 23
        self.fs.setcontents("info.txt", test_str)
        size = self.fs.getsize("info.txt")
        self.assertEqual(size, len(test_str))

    def test_movefile(self):
        check = self.check
        contents = b(
            "If the implementation is hard to explain, it's a bad idea.")

        def makefile(path):
            self.fs.setcontents(path, contents)

        def checkcontents(path):
            check_contents = self.fs.getcontents(path, "rb")
            self.assertEqual(check_contents, contents)
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
        self.assertRaises(
            DestinationExistsError, self.fs.move, "foo/bar/a.txt", "/c.txt")
        self.assert_(check("foo/bar/a.txt"))
        self.assert_(check("/c.txt"))
        self.fs.move("foo/bar/a.txt", "/c.txt", overwrite=True)
        self.assert_(not check("foo/bar/a.txt"))
        self.assert_(check("/c.txt"))

    def test_movedir(self):
        check = self.check
        contents = b(
            "If the implementation is hard to explain, it's a bad idea.")

        def makefile(path):
            self.fs.setcontents(path, contents)

        self.assertRaises(ResourceNotFoundError, self.fs.movedir, "a", "b")
        self.fs.makedir("a")
        self.fs.makedir("b")
        makefile("a/1.txt")
        makefile("a/2.txt")
        makefile("a/3.txt")
        self.fs.makedir("a/foo/bar", recursive=True)
        makefile("a/foo/bar/baz.txt")

        self.fs.movedir("a", "copy of a")

        self.assert_(self.fs.isdir("copy of a"))
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
        self.assertRaises(
            DestinationExistsError, self.fs.movedir, "copy of a", "a")
        self.fs.movedir("copy of a", "a", overwrite=True)
        self.assert_(not check("copy of a"))
        self.assert_(check("a/1.txt"))
        self.assert_(check("a/2.txt"))
        self.assert_(check("a/3.txt"))
        self.assert_(check("a/foo/bar/baz.txt"))

    def test_cant_copy_from_os(self):
        sys_executable = os.path.abspath(os.path.realpath(sys.executable))
        self.assertRaises(FSError, self.fs.copy, sys_executable, "py.exe")

    def test_copyfile(self):
        check = self.check
        contents = b(
            "If the implementation is hard to explain, it's a bad idea.")

        def makefile(path, contents=contents):
            self.fs.setcontents(path, contents)

        def checkcontents(path, contents=contents):
            check_contents = self.fs.getcontents(path, "rb")
            self.assertEqual(check_contents, contents)
            return contents == check_contents

        self.fs.makedir("foo/bar", recursive=True)
        makefile("foo/bar/a.txt")
        self.assert_(check("foo/bar/a.txt"))
        self.assert_(checkcontents("foo/bar/a.txt"))
        # import rpdb2; rpdb2.start_embedded_debugger('password');
        self.fs.copy("foo/bar/a.txt", "foo/b.txt")
        self.assert_(check("foo/bar/a.txt"))
        self.assert_(check("foo/b.txt"))
        self.assert_(checkcontents("foo/bar/a.txt"))
        self.assert_(checkcontents("foo/b.txt"))

        self.fs.copy("foo/b.txt", "c.txt")
        self.assert_(check("foo/b.txt"))
        self.assert_(check("/c.txt"))
        self.assert_(checkcontents("/c.txt"))

        makefile("foo/bar/a.txt", b("different contents"))
        self.assert_(checkcontents("foo/bar/a.txt", b("different contents")))
        self.assertRaises(
            DestinationExistsError, self.fs.copy, "foo/bar/a.txt", "/c.txt")
        self.assert_(checkcontents("/c.txt"))
        self.fs.copy("foo/bar/a.txt", "/c.txt", overwrite=True)
        self.assert_(checkcontents("foo/bar/a.txt", b("different contents")))
        self.assert_(checkcontents("/c.txt", b("different contents")))

    def test_copydir(self):
        check = self.check
        contents = b(
            "If the implementation is hard to explain, it's a bad idea.")

        def makefile(path):
            self.fs.setcontents(path, contents)

        def checkcontents(path):
            check_contents = self.fs.getcontents(path)
            self.assertEqual(check_contents, contents)
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

        self.assertRaises(DestinationExistsError, self.fs.copydir, "a", "b")
        self.fs.copydir("a", "b", overwrite=True)
        self.assert_(check("b/1.txt"))
        self.assert_(check("b/2.txt"))
        self.assert_(check("b/3.txt"))
        self.assert_(check("b/foo/bar/baz.txt"))
        checkcontents("b/1.txt")

    def test_copydir_with_dotfile(self):
        check = self.check
        contents = b(
            "If the implementation is hard to explain, it's a bad idea.")

        def makefile(path):
            self.fs.setcontents(path, contents)

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
            read_contents = self.fs.getcontents(path, "rb")
            self.assertEqual(read_contents, check_contents)
            return read_contents == check_contents
        test_strings = [b("Beautiful is better than ugly."),
                        b("Explicit is better than implicit."),
                        b("Simple is better than complex.")]
        all_strings = b("").join(test_strings)

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
        # On win32, tell() gives zero until you actually write to the file
        # self.assertEquals(f3.tell(),len(test_strings[0]))
        f3.write(test_strings[1])
        self.assertEquals(f3.tell(), len(test_strings[0])+len(test_strings[1]))
        f3.write(test_strings[2])
        self.assertEquals(f3.tell(), len(all_strings))
        f3.close()
        self.assert_(checkcontents("b.txt", all_strings))
        f4 = self.fs.open("b.txt", "wb")
        f4.write(test_strings[2])
        f4.close()
        self.assert_(checkcontents("b.txt", test_strings[2]))
        f5 = self.fs.open("c.txt", "wb")
        for s in test_strings:
            f5.write(s+b("\n"))
        f5.close()
        f6 = self.fs.open("c.txt", "rb")
        for s, t in zip(f6, test_strings):
            self.assertEqual(s, t+b("\n"))
        f6.close()
        f7 = self.fs.open("c.txt", "rb")
        f7.seek(13)
        word = f7.read(6)
        self.assertEqual(word, b("better"))
        f7.seek(1, os.SEEK_CUR)
        word = f7.read(4)
        self.assertEqual(word, b("than"))
        f7.seek(-9, os.SEEK_END)
        word = f7.read(7)
        self.assertEqual(word, b("complex"))
        f7.close()
        self.assertEqual(self.fs.getcontents("a.txt", "rb"), all_strings)

    def test_truncate(self):
        def checkcontents(path, check_contents):
            read_contents = self.fs.getcontents(path, "rb")
            self.assertEqual(read_contents, check_contents)
            return read_contents == check_contents
        self.fs.setcontents("hello", b("world"))
        checkcontents("hello", b("world"))
        self.fs.setcontents("hello", b("hi"))
        checkcontents("hello", b("hi"))
        self.fs.setcontents("hello", b("1234567890"))
        checkcontents("hello", b("1234567890"))
        with self.fs.open("hello", "rb+") as f:
            f.truncate(7)
        checkcontents("hello", b("1234567"))
        with self.fs.open("hello", "rb+") as f:
            f.seek(5)
            f.truncate()
        checkcontents("hello", b("12345"))

    def test_truncate_to_larger_size(self):
        with self.fs.open("hello", "wb") as f:
            f.truncate(30)

        self.assertEquals(self.fs.getsize("hello"), 30)

        # Some file systems (FTPFS) don't support both reading and writing
        if self.fs.getmeta('file.read_and_write', True):
            with self.fs.open("hello", "rb+") as f:
                f.seek(25)
                f.write(b("123456"))

            with self.fs.open("hello", "rb") as f:
                f.seek(25)
                self.assertEquals(f.read(), b("123456"))

    def test_write_past_end_of_file(self):
        if self.fs.getmeta('file.read_and_write', True):
            with self.fs.open("write_at_end", "wb") as f:
                f.seek(25)
                f.write(b("EOF"))
            with self.fs.open("write_at_end", "rb") as f:
                self.assertEquals(f.read(), b("\x00")*25 + b("EOF"))

    def test_with_statement(self):
        #  This is a little tricky since 'with' is actually new syntax.
        #  We use eval() to make this method safe for old python versions.
        import sys
        if sys.version_info[0] >= 2 and sys.version_info[1] >= 5:
            #  A successful 'with' statement
            contents = "testing the with statement"
            code = "from __future__ import with_statement\n"
            code += "with self.fs.open('f.txt','wb-') as testfile:\n"
            code += "    testfile.write(contents)\n"
            code += "self.assertEquals(self.fs.getcontents('f.txt', 'rb'),contents)"
            code = compile(code, "<string>", 'exec')
            eval(code)
            # A 'with' statement raising an error
            contents = "testing the with statement"
            code = "from __future__ import with_statement\n"
            code += "with self.fs.open('f.txt','wb-') as testfile:\n"
            code += "    testfile.write(contents)\n"
            code += "    raise ValueError\n"
            code = compile(code, "<string>", 'exec')
            self.assertRaises(ValueError, eval, code, globals(), locals())
            self.assertEquals(self.fs.getcontents('f.txt', 'rb'), contents)

    def test_pickling(self):
        if self.fs.getmeta('pickle_contents', True):
            self.fs.setcontents("test1", b("hello world"))
            fs2 = pickle.loads(pickle.dumps(self.fs))
            self.assert_(fs2.isfile("test1"))
            fs3 = pickle.loads(pickle.dumps(self.fs, -1))
            self.assert_(fs3.isfile("test1"))
        else:
            # Just make sure it doesn't throw an exception
            fs2 = pickle.loads(pickle.dumps(self.fs))

    def test_big_file(self):
        """Test handling of a big file (1MB)"""
        chunk_size = 1024 * 256
        num_chunks = 4

        def chunk_stream():
            """Generate predictable-but-randomy binary content."""
            r = random.Random(0)
            randint = r.randint
            int2byte = six.int2byte
            for _i in xrange(num_chunks):
                c = b("").join(int2byte(randint(
                    0, 255)) for _j in xrange(chunk_size//8))
                yield c * 8
                f = self.fs.open("bigfile", "wb")
                try:
                    for chunk in chunk_stream():
                        f.write(chunk)
                finally:
                    f.close()
                chunks = chunk_stream()
                f = self.fs.open("bigfile", "rb")
                try:
                    try:
                        while True:
                            if chunks.next() != f.read(chunk_size):
                                assert False, "bigfile was corrupted"
                    except StopIteration:
                        if f.read() != b(""):
                            assert False, "bigfile was corrupted"
                finally:
                    f.close()

    def test_settimes(self):
        def cmp_datetimes(d1, d2):
            """Test datetime objects are the same to within the timestamp accuracy"""
            dts1 = time.mktime(d1.timetuple())
            dts2 = time.mktime(d2.timetuple())
            return int(dts1) == int(dts2)
        d1 = datetime.datetime(2010, 6, 20, 11, 0, 9, 987699)
        d2 = datetime.datetime(2010, 7, 5, 11, 0, 9, 500000)
        self.fs.setcontents('/dates.txt', b('check dates'))
        # If the implementation supports settimes, check that the times
        # can be set and then retrieved
        try:
            self.fs.settimes('/dates.txt', d1, d2)
        except UnsupportedError:
            pass
        else:
            info = self.fs.getinfo('/dates.txt')
            self.assertTrue(cmp_datetimes(d1, info['accessed_time']))
            self.assertTrue(cmp_datetimes(d2, info['modified_time']))

    def test_removeroot(self):
        self.assertRaises(RemoveRootError, self.fs.removedir, "/")

    def test_zero_read(self):
        """Test read(0) returns empty string"""
        self.fs.setcontents('foo.txt', b('Hello, World'))
        with self.fs.open('foo.txt', 'rb') as f:
            self.assert_(len(f.read(0)) == 0)
        with self.fs.open('foo.txt', 'rt') as f:
            self.assert_(len(f.read(0)) == 0)

# May be disabled - see end of file


class ThreadingTestCases(object):
    """Testcases for thread-safety of FS implementations."""

    #  These are either too slow to be worth repeating,
    #  or cannot possibly break cross-thread.
    _dont_retest = ("test_pickling", "test_multiple_overwrite",)

    __lock = threading.RLock()

    def _yield(self):
        # time.sleep(0.001)
        # Yields without a delay
        time.sleep(0)

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()

    def _makeThread(self, func, errors):
        def runThread():
            try:
                func()
            except Exception:
                errors.append(sys.exc_info())
        thread = threading.Thread(target=runThread)
        thread.daemon = True
        return thread

    def _runThreads(self, *funcs):
        check_interval = sys.getcheckinterval()
        sys.setcheckinterval(1)
        try:
            errors = []
            threads = [self._makeThread(f, errors) for f in funcs]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            for (c, e, t) in errors:
                raise e, None, t
        finally:
            sys.setcheckinterval(check_interval)

    def test_setcontents_threaded(self):
        def setcontents(name, contents):
            f = self.fs.open(name, "wb")
            self._yield()
            try:
                f.write(contents)
                self._yield()
            finally:
                f.close()

        def thread1():
            c = b("thread1 was 'ere")
            setcontents("thread1.txt", c)
            self.assertEquals(self.fs.getcontents("thread1.txt", 'rb'), c)

        def thread2():
            c = b("thread2 was 'ere")
            setcontents("thread2.txt", c)
            self.assertEquals(self.fs.getcontents("thread2.txt", 'rb'), c)
        self._runThreads(thread1, thread2)

    def test_setcontents_threaded_samefile(self):
        def setcontents(name, contents):
            f = self.fs.open(name, "wb")
            self._yield()
            try:
                f.write(contents)
                self._yield()
            finally:
                f.close()

        def thread1():
            c = b("thread1 was 'ere")
            setcontents("threads.txt", c)
            self._yield()
            self.assertEquals(self.fs.listdir("/"), ["threads.txt"])

        def thread2():
            c = b("thread2 was 'ere")
            setcontents("threads.txt", c)
            self._yield()
            self.assertEquals(self.fs.listdir("/"), ["threads.txt"])

        def thread3():
            c = b("thread3 was 'ere")
            setcontents("threads.txt", c)
            self._yield()
            self.assertEquals(self.fs.listdir("/"), ["threads.txt"])
        try:
            self._runThreads(thread1, thread2, thread3)
        except ResourceLockedError:
            # that's ok, some implementations don't support concurrent writes
            pass

    def test_cases_in_separate_dirs(self):
        class TestCases_in_subdir(self.__class__, unittest.TestCase):
            """Run all testcases against a subdir of self.fs"""
            def __init__(this, subdir):
                super(TestCases_in_subdir, this).__init__("test_listdir")
                this.subdir = subdir
                for meth in dir(this):
                    if not meth.startswith("test_"):
                        continue
                    if meth in self._dont_retest:
                        continue
                    if not hasattr(FSTestCases, meth):
                        continue
                    if self.fs.exists(subdir):
                        self.fs.removedir(subdir, force=True)
                    self.assertFalse(self.fs.isdir(subdir))
                    self.assertTrue(self.fs.isdir("/"))
                    self.fs.makedir(subdir)
                    self._yield()
                    getattr(this, meth)()

            @property
            def fs(this):
                return self.fs.opendir(this.subdir)

            def check(this, p):
                return self.check(pathjoin(this.subdir, relpath(p)))

        def thread1():
            TestCases_in_subdir("thread1")

        def thread2():
            TestCases_in_subdir("thread2")

        def thread3():
            TestCases_in_subdir("thread3")
        self._runThreads(thread1, thread2, thread3)

    def test_makedir_winner(self):
        errors = []

        def makedir():
            try:
                self.fs.makedir("testdir")
            except DestinationExistsError, e:
                errors.append(e)

        def makedir_noerror():
            try:
                self.fs.makedir("testdir", allow_recreate=True)
            except DestinationExistsError, e:
                errors.append(e)

        def removedir():
            try:
                self.fs.removedir("testdir")
            except (ResourceNotFoundError, ResourceLockedError), e:
                errors.append(e)
        # One thread should succeed, one should error
        self._runThreads(makedir, makedir)
        self.assertEquals(len(errors), 1)
        self.fs.removedir("testdir")
        # One thread should succeed, two should error
        errors = []
        self._runThreads(makedir, makedir, makedir)
        if len(errors) != 2:
            raise AssertionError(errors)
        self.fs.removedir("testdir")
        # All threads should succeed
        errors = []
        self._runThreads(makedir_noerror, makedir_noerror, makedir_noerror)
        self.assertEquals(len(errors), 0)
        self.assertTrue(self.fs.isdir("testdir"))
        self.fs.removedir("testdir")
        # makedir() can beat removedir() and vice-versa
        errors = []
        self._runThreads(makedir, removedir)
        if self.fs.isdir("testdir"):
            self.assertEquals(len(errors), 1)
            self.assertFalse(isinstance(errors[0], DestinationExistsError))
            self.fs.removedir("testdir")
        else:
            self.assertEquals(len(errors), 0)

    def test_concurrent_copydir(self):
        self.fs.makedir("a")
        self.fs.makedir("a/b")
        self.fs.setcontents("a/hello.txt", b("hello world"))
        self.fs.setcontents("a/guido.txt", b("is a space alien"))
        self.fs.setcontents("a/b/parrot.txt", b("pining for the fiords"))

        def copydir():
            self._yield()
            self.fs.copydir("a", "copy of a")

        def copydir_overwrite():
            self._yield()
            self.fs.copydir("a", "copy of a", overwrite=True)
        # This should error out since we're not overwriting
        self.assertRaises(
            DestinationExistsError, self._runThreads, copydir, copydir)
        self.assert_(self.fs.isdir('a'))
        self.assert_(self.fs.isdir('a'))
        copydir_overwrite()
        self.assert_(self.fs.isdir('a'))
        # This should run to completion and give a valid state, unless
        # files get locked when written to.
        try:
            self._runThreads(copydir_overwrite, copydir_overwrite)
        except ResourceLockedError:
            pass
        self.assertTrue(self.fs.isdir("copy of a"))
        self.assertTrue(self.fs.isdir("copy of a/b"))
        self.assertEqual(self.fs.getcontents(
            "copy of a/b/parrot.txt", 'rb'), b("pining for the fiords"))
        self.assertEqual(self.fs.getcontents(
            "copy of a/hello.txt", 'rb'), b("hello world"))
        self.assertEqual(self.fs.getcontents(
            "copy of a/guido.txt", 'rb'), b("is a space alien"))

    def test_multiple_overwrite(self):
        contents = [b("contents one"), b(
            "contents the second"), b("number three")]

        def thread1():
            for i in xrange(30):
                for c in contents:
                    self.fs.setcontents("thread1.txt", c)
                    self.assertEquals(self.fs.getsize("thread1.txt"), len(c))
                    self.assertEquals(self.fs.getcontents(
                        "thread1.txt", 'rb'), c)

        def thread2():
            for i in xrange(30):
                for c in contents:
                    self.fs.setcontents("thread2.txt", c)
                    self.assertEquals(self.fs.getsize("thread2.txt"), len(c))
                    self.assertEquals(self.fs.getcontents(
                        "thread2.txt", 'rb'), c)
        self._runThreads(thread1, thread2)

# Uncomment to temporarily disable threading tests
# class ThreadingTestCases(object):
#    _dont_retest = ()
