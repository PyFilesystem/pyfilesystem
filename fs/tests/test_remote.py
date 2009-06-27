"""

  fs.tests.test_remote:  testcases for FS remote support

"""

from fs.tests import FSTestCases, ThreadingTestCases

import unittest

from fs.remote import *

from fs.tempfs import TempFS
from fs.path import *

class TestCacheFS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    def setUp(self):
        sys.setcheckinterval(1)
        self.fs = CacheFS(TempFS())

    def tearDown(self):
        self.fs.close()

