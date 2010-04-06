"""

  fs.tests.test_path:  testcases for the fs path functions

"""


import unittest
import fs.tests

from fs.path import *

class TestPathFunctions(unittest.TestCase):
    """Testcases for FS path functions."""

    def test_normpath(self):
        tests = [   ("\\a\\b\\c", "/a/b/c"),
                    ("", ""),
                    ("/a/b/c", "/a/b/c"),
                    ("a/b/c", "a/b/c"),
                    ("a/b/../c/", "a/c"),
                    ("/","/"),
                    (u"a/\N{GREEK SMALL LETTER BETA}\\c",u"a/\N{GREEK SMALL LETTER BETA}/c"),
                    ]
        for path, result in tests:
            self.assertEqual(normpath(path), result)

    def test_pathjoin(self):
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
                    (u"a/\N{GREEK SMALL LETTER BETA}","c",u"a/\N{GREEK SMALL LETTER BETA}/c"),
        ]
        for testpaths in tests:
            paths = testpaths[:-1]
            result = testpaths[-1]
            self.assertEqual(fs.pathjoin(*paths), result)

        self.assertRaises(ValueError, fs.pathjoin, "../")
        self.assertRaises(ValueError, fs.pathjoin, "./../")
        self.assertRaises(ValueError, fs.pathjoin, "a/b", "../../..")
        self.assertRaises(ValueError, fs.pathjoin, "a/b/../../../d")

    def test_relpath(self):
        tests = [   ("/a/b", "a/b"),
                    ("a/b", "a/b"),
                    ("/", "") ]

        for path, result in tests:
            self.assertEqual(fs.relpath(path), result)

    def test_abspath(self):
        tests = [   ("/a/b", "/a/b"),
                    ("a/b", "/a/b"),
                    ("/", "/") ]

        for path, result in tests:
            self.assertEqual(fs.abspath(path), result)

    def test_iteratepath(self):
        tests = [   ("a/b", ["a", "b"]),
                    ("", [] ),
                    ("aaa/bbb/ccc", ["aaa", "bbb", "ccc"]),
                    ("a/b/c/../d", ["a", "b", "d"]) ]

        for path, results in tests:
            for path_component, expected in zip(iteratepath(path), results):
                self.assertEqual(path_component, expected)

        self.assertEqual(list(iteratepath("a/b/c/d", 1)), ["a", "b/c/d"])
        self.assertEqual(list(iteratepath("a/b/c/d", 2)), ["a", "b", "c/d"])

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


class Test_PathMap(unittest.TestCase):

    def test_basics(self):
        map = PathMap()
        map["hello"] = "world"
        self.assertEquals(map["/hello"],"world")
        self.assertEquals(map["/hello/"],"world")
        self.assertEquals(map.get("hello"),"world")

    def test_iteration(self):
        map = PathMap()
        map["hello/world"] = 1
        map["hello/world/howareya"] = 2
        map["hello/world/iamfine"] = 3
        map["hello/kitty"] = 4
        map["hello/kitty/islame"] = 5
        map["batman/isawesome"] = 6
        self.assertEquals(set(map.iterkeys()),set(("/hello/world","/hello/world/howareya","/hello/world/iamfine","/hello/kitty","/hello/kitty/islame","/batman/isawesome")))
        self.assertEquals(sorted(map.values()),range(1,7))
        self.assertEquals(sorted(map.items("/hello/world/")),[("/hello/world",1),("/hello/world/howareya",2),("/hello/world/iamfine",3)])
        self.assertEquals(zip(map.keys(),map.values()),map.items())
        self.assertEquals(zip(map.keys("batman"),map.values("batman")),map.items("batman"))
        self.assertEquals(set(map.iternames("hello")),set(("world","kitty")))
        self.assertEquals(set(map.iternames("/hello/kitty")),set(("islame",)))


