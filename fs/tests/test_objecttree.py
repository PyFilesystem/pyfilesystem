"""

  fs.tests.test_objectree:  testcases for the fs objecttree module

"""


import unittest

import fs.tests
from fs import objecttree

class TestObjectTree(unittest.TestCase):
    """Testcases for the ObjectTree class."""

    def test_getset(self):
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

