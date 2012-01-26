"""

  fs.tests.test_opener:  testcases for FS opener

"""

import unittest
import tempfile
import shutil

from fs.opener import opener
from fs import path

class TestOpener(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(u"fstest_opener")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def testOpen(self):
        filename = path.join(self.temp_dir, 'foo.txt')
        file_object = opener.open(filename, 'wb')
        file_object.close()
        self.assertTrue(file_object.closed)






