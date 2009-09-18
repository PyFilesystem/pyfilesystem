"""

  fs.tests.test_errors:  testcases for the fs error classes functions

"""


import unittest
import fs.tests
from fs.errors import *
import pickle

from fs.path import *

class TestErrorPickling(unittest.TestCase):

    def test_pickling(self):
        def assert_dump_load(e):
            e2 = pickle.loads(pickle.dumps(e))
            self.assertEqual(e.__dict__,e2.__dict__)
        assert_dump_load(FSError())
        assert_dump_load(PathError("/some/path"))
        assert_dump_load(ResourceNotFoundError("/some/other/path"))
        assert_dump_load(UnsupportedError("makepony"))


