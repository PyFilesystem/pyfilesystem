#!/usr/bin/env python

import unittest
import fs

class TestHelpers(unittest.TestCase):

    def test_isabsolutepath(self):
        tests = [   ('', False),
                    ('/', True),
                    ('/A/B', True),
                    ('/asdasd', True),
                    ('a/b/c', False),
                    ]
        for path, result in tests:
            self.assertEqual(fs.isabsolutepath(path), result)

    def test_normpath(self):
        tests = [   ("\\a\\b\\c", "/a/b/c"),
                    ("", ""),
                    ("/a/b/c", "/a/b/c"),
                    ]
        for path, result in tests:
            self.assertEqual(fs.normpath(path), result)

    def test_pathjon(self):
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

        tests = [   ("/a/b", "a/b"),
                    ("a/b", "a/b"),
                    ("/", "") ]
        
        for path, result in tests:
            print path, result
            self.assertEqual(fs.makerelative(path), result)

    def test_absolute(self):

        tests = [   ("/a/b", "/a/b"),
                    ("a/b", "/a/b"),
                    ("/", "/") ]

        for path, result in tests:
            self.assertEqual(fs.makeabsolute(path), result)

    def test_iteratepath(self):

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

if __name__ == "__main__":
    import nose
    nose.run()