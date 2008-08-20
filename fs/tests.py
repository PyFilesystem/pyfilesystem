#!/usr/bin/env python

import unittest
import fs
import shutil

class TestHelpers(unittest.TestCase):

    def test_isabsolutepath(self):
        """fs.isabsolutepath tests"""
        tests = [   ('', False),
                    ('/', True),
                    ('/A/B', True),
                    ('/asdasd', True),
                    ('a/b/c', False),
                    ]
        for path, result in tests:
            self.assertEqual(fs.isabsolutepath(path), result)

    def test_normpath(self):
        """fs.normpath tests"""
        tests = [   ("\\a\\b\\c", "/a/b/c"),
                    ("", ""),
                    ("/a/b/c", "/a/b/c"),
                    ]
        for path, result in tests:
            self.assertEqual(fs.normpath(path), result)

    def test_pathjon(self):
        """fs.pathjoin tests"""
        tests = [   ("", "a", "a"),
                    ("a", "a", "a/a"),
                    ("a/b", "../c", "a/c"),
                    ("a/b/../c", "d", "a/c/d"),
                    ("/a/b/c", "d", "/a/b/c/d"),
                    ("/a/b/c", "../../../d", "/d"),
                    ("a", "b", "c", "a/b/c"),
                    ("a/b/c", "../d", "c", "a/b/d/c"),
                    ("a/b/c", "../d", "/a", "/a"),
                    ("aaa", "bbb/ccc", "aaa/bbb/ccc"),
                    ("aaa", "bbb\ccc", "aaa/bbb/ccc"),
                    ("aaa", "bbb", "ccc", "/aaa", "eee", "/aaa/eee"),
                    ("a/b", "./d", "e", "a/b/d/e"),
                    ("/", "/", "/"),
                    ("/", "", "/"),
        ]
        for testpaths in tests:
            paths = testpaths[:-1]
            result = testpaths[-1]
            self.assertEqual(fs.pathjoin(*paths), result)

        self.assertRaises(fs.PathError, fs.pathjoin, "../")
        self.assertRaises(fs.PathError, fs.pathjoin, "./../")
        self.assertRaises(fs.PathError, fs.pathjoin, "a/b", "../../..")
        self.assertRaises(fs.PathError, fs.pathjoin, "a/b/../../../d")

    def test_makerelative(self):
        """fs.makerelative tests"""
        tests = [   ("/a/b", "a/b"),
                    ("a/b", "a/b"),
                    ("/", "") ]

        for path, result in tests:
            print path, result
            self.assertEqual(fs.makerelative(path), result)

    def test_makeabsolute(self):
        """fs.makeabsolute tests"""
        tests = [   ("/a/b", "/a/b"),
                    ("a/b", "/a/b"),
                    ("/", "/") ]

        for path, result in tests:
            self.assertEqual(fs.makeabsolute(path), result)

    def test_iteratepath(self):
        """fs.iteratepath tests"""
        tests = [   ("a/b", ["a", "b"]),
                    ("", [] ),
                    ("aaa/bbb/ccc", ["aaa", "bbb", "ccc"]),
                    ("a/b/c/../d", ["a", "b", "d"]) ]

        for path, results in tests:
            print repr(path), results
            for path_component, expected in zip(fs._iteratepath(path), results):
                self.assertEqual(path_component, expected)

        self.assertEqual(list(fs._iteratepath("a/b/c/d", 1)), ["a", "b/c/d"])
        self.assertEqual(list(fs._iteratepath("a/b/c/d", 2)), ["a", "b", "c/d"])

    def test_pathsplit(self):
        """fs.pathsplit tests"""
        tests = [   ("a/b", ("a", "b")),
                    ("a/b/c", ("a/b", "c")),
                    ("a", ("", "a")),
                    ("", ("", "")),
                    ("/", ("", "")),
                    ("foo/bar", ("foo", "bar")),
                    ("foo/bar/baz", ("foo/bar", "baz")),
                ]
        for path, result in tests:
            self.assertEqual(fs.pathsplit(path), result)


import objecttree

class TestObjectTree(unittest.TestCase):

    def test_getset(self):
        """objecttree.ObjectTree tests"""
        ot = objecttree.ObjectTree()
        ot['foo'] = "bar"
        self.assertEqual(ot['foo'], 'bar')

        ot = objecttree.ObjectTree()
        ot['foo/bar'] = "baz"
        self.assertEqual(ot['foo'], {'bar':'baz'})
        self.assertEqual(ot['foo/bar'], 'baz')

        del ot['foo/bar']
        self.assertEqual(ot['foo'], {})

        ot = objecttree.ObjectTree()
        ot['a/b/c'] = "A"
        ot['a/b/d'] = "B"
        ot['a/b/e'] = "C"
        ot['a/b/f'] = "D"
        self.assertEqual(sorted(ot['a/b'].values()), ['A', 'B', 'C', 'D'])
        self.assert_(ot.get('a/b/x', -1) == -1)

        self.assert_('a/b/c' in ot)
        self.assert_('a/b/x' not in ot)
        self.assert_(ot.isobject('a/b/c'))
        self.assert_(ot.isobject('a/b/d'))
        self.assert_(not ot.isobject('a/b'))

        left, object, right = ot.partialget('a/b/e/f/g')
        self.assertEqual(left, "a/b/e")
        self.assertEqual(object, "C")
        self.assertEqual(right, "f/g")



import tempfile
import osfs
import os

class TestFS(unittest.TestCase):

    def setUp(self):

        self.temp_dir = tempfile.mkdtemp("fstest")
        self.fs = osfs.OSFS(self.temp_dir)
        print "Temp dir is", self.temp_dir

    def tearDown(self):
        assert "fstest" in self.temp_dir
        shutil.rmtree(self.temp_dir)

    def check(self, p):
        return os.path.exists(os.path.join(self.temp_dir, p))

    def test_makedir(self):
        """osfs.makedir tests"""
        check = self.check

        self.fs.makedir("a")
        self.assert_(check("a"))
        self.assertRaises(fs.FSError, self.fs.makedir, "a/b/c")

        self.fs.makedir("a/b/c", recursive=True)
        self.assert_(check("a/b/c"))

        self.fs.makedir("foo/bar/baz", recursive=True)
        self.assert_(check("foo/bar/baz"))

        self.fs.makedir("a/b/child")
        self.assert_(check("a/b/child"))


    def test_removedir(self):
        """osfs.removedir tests"""
        check = self.check
        self.fs.makedir("a")
        self.assert_(check("a"))
        self.fs.removedir("a")
        self.assert_(not check("a"))
        self.fs.makedir("a/b/c/d", recursive=True)
        self.assertRaises(fs.FSError, self.fs.removedir, "a/b")
        self.fs.removedir("a/b/c/d")
        self.assert_(not check("a/b/c/d"))
        self.fs.removedir("a/b/c")
        self.assert_(not check("a/b/c"))
        self.fs.removedir("a/b")
        self.assert_(not check("a/b"))

        self.fs.makedir("foo/bar/baz", recursive=True)
        self.fs.removedir("foo/bar/baz", recursive=True)
        self.assert_(not check("foo/bar/baz"))
        self.assert_(not check("foo/bar"))
        self.assert_(not check("foo"))

    def test_rename(self):
        """osfs.rename tests"""
        check = self.check
        self.fs.open("foo.txt", 'wt').write("Hello, World!")
        self.assert_(check("foo.txt"))
        self.fs.rename("foo.txt", "bar.txt")
        self.assert_(check("bar.txt"))


if __name__ == "__main__":
    #t = TestFS()
    #t.setUp()
    #t.tearDown()
    import nose
    nose.main()