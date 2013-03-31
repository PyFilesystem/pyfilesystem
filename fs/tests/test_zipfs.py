"""

  fs.tests.test_zipfs:  testcases for the ZipFS class

"""

import unittest
import os
import random
import zipfile
import tempfile
import shutil

import fs.tests
from fs.path import *
from fs import zipfs

from six import PY3, b


class TestReadZipFS(unittest.TestCase):

    def setUp(self):
        self.temp_filename = "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(6))+".zip"
        self.temp_filename = os.path.join(tempfile.gettempdir(), self.temp_filename)

        self.zf = zipfile.ZipFile(self.temp_filename, "w")
        zf = self.zf
        zf.writestr("a.txt", b("Hello, World!"))
        zf.writestr("b.txt", b("b"))
        zf.writestr("1.txt", b("1"))
        zf.writestr("foo/bar/baz.txt", b("baz"))
        zf.writestr("foo/second.txt", b("hai"))
        zf.close()
        self.fs = zipfs.ZipFS(self.temp_filename, "r")

    def tearDown(self):
        self.fs.close()
        os.remove(self.temp_filename)

    def check(self, p):
        try:
            self.zipfile.getinfo(p)
            return True
        except:
            return False

    def test_reads(self):
        def read_contents(path):
            f = self.fs.open(path, 'rb')
            contents = f.read()
            return contents

        def check_contents(path, expected):
            self.assert_(read_contents(path) == expected)
        check_contents("a.txt", b("Hello, World!"))
        check_contents("1.txt", b("1"))
        check_contents("foo/bar/baz.txt", b("baz"))

    def test_getcontents(self):
        def read_contents(path):
            return self.fs.getcontents(path, 'rb')

        def check_contents(path, expected):
            self.assert_(read_contents(path) == expected)
        check_contents("a.txt", b("Hello, World!"))
        check_contents("1.txt", b("1"))
        check_contents("foo/bar/baz.txt", b("baz"))

    def test_is(self):
        self.assert_(self.fs.isfile('a.txt'))
        self.assert_(self.fs.isfile('1.txt'))
        self.assert_(self.fs.isfile('foo/bar/baz.txt'))
        self.assert_(self.fs.isdir('foo'))
        self.assert_(self.fs.isdir('foo/bar'))
        self.assert_(self.fs.exists('a.txt'))
        self.assert_(self.fs.exists('1.txt'))
        self.assert_(self.fs.exists('foo/bar/baz.txt'))
        self.assert_(self.fs.exists('foo'))
        self.assert_(self.fs.exists('foo/bar'))

    def test_listdir(self):

        def check_listing(path, expected):
            dir_list = self.fs.listdir(path)
            self.assert_(sorted(dir_list) == sorted(expected))
            for item in dir_list:
                self.assert_(isinstance(item, unicode))
        check_listing('/', ['a.txt', '1.txt', 'foo', 'b.txt'])
        check_listing('foo', ['second.txt', 'bar'])
        check_listing('foo/bar', ['baz.txt'])


class TestWriteZipFS(unittest.TestCase):

    def setUp(self):
        self.temp_filename = "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(6))+".zip"
        self.temp_filename = os.path.join(tempfile.gettempdir(), self.temp_filename)

        zip_fs = zipfs.ZipFS(self.temp_filename, 'w')

        def makefile(filename, contents):
            if dirname(filename):
                zip_fs.makedir(dirname(filename), recursive=True, allow_recreate=True)
            f = zip_fs.open(filename, 'wb')
            f.write(contents)
            f.close()

        makefile("a.txt", b("Hello, World!"))
        makefile("b.txt", b("b"))
        makefile(u"\N{GREEK SMALL LETTER ALPHA}/\N{GREEK CAPITAL LETTER OMEGA}.txt", b("this is the alpha and the omega"))
        makefile("foo/bar/baz.txt", b("baz"))
        makefile("foo/second.txt", b("hai"))

        zip_fs.close()

    def tearDown(self):
        os.remove(self.temp_filename)

    def test_valid(self):
        zf = zipfile.ZipFile(self.temp_filename, "r")
        self.assert_(zf.testzip() is None)
        zf.close()

    def test_creation(self):
        zf = zipfile.ZipFile(self.temp_filename, "r")
        def check_contents(filename, contents):
            if PY3:
                zcontents = zf.read(filename)
            else:
                zcontents = zf.read(filename.encode("CP437"))
            self.assertEqual(contents, zcontents)
        check_contents("a.txt", b("Hello, World!"))
        check_contents("b.txt", b("b"))
        check_contents("foo/bar/baz.txt", b("baz"))
        check_contents("foo/second.txt", b("hai"))
        check_contents(u"\N{GREEK SMALL LETTER ALPHA}/\N{GREEK CAPITAL LETTER OMEGA}.txt", b("this is the alpha and the omega"))


class TestAppendZipFS(TestWriteZipFS):

    def setUp(self):
        self.temp_filename = "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(6))+".zip"
        self.temp_filename = os.path.join(tempfile.gettempdir(), self.temp_filename)

        zip_fs = zipfs.ZipFS(self.temp_filename, 'w')

        def makefile(filename, contents):
            if dirname(filename):
                zip_fs.makedir(dirname(filename), recursive=True, allow_recreate=True)
            f = zip_fs.open(filename, 'wb')
            f.write(contents)
            f.close()

        makefile("a.txt", b("Hello, World!"))
        makefile("b.txt", b("b"))

        zip_fs.close()
        zip_fs = zipfs.ZipFS(self.temp_filename, 'a')

        makefile("foo/bar/baz.txt", b("baz"))
        makefile(u"\N{GREEK SMALL LETTER ALPHA}/\N{GREEK CAPITAL LETTER OMEGA}.txt", b("this is the alpha and the omega"))
        makefile("foo/second.txt", b("hai"))

        zip_fs.close()

class TestZipFSErrors(unittest.TestCase):

    def setUp(self):
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.workdir)

    def test_bogus_zipfile(self):
        badzip = os.path.join(self.workdir,"bad.zip")
        f = open(badzip,"wb")
        f.write(b("I'm not really a zipfile"))
        f.close()
        self.assertRaises(zipfs.ZipOpenError,zipfs.ZipFS,badzip)

    def test_missing_zipfile(self):
        missingzip = os.path.join(self.workdir,"missing.zip")
        self.assertRaises(zipfs.ZipNotFoundError,zipfs.ZipFS,missingzip)

