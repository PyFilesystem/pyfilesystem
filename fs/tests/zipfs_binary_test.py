"""
Test case for ZipFS binary file reading/writing
Passes ok on Linux, fails on Windows (tested: Win7, 64-bit):

AssertionError: ' \r\n' != ' \n'
"""

import unittest
from fs.zipfs import ZipFS
import os

from six import b

class ZipFsBinaryWriteRead(unittest.TestCase):
    test_content = b(chr(32) + chr(10))
    
    def setUp(self):
        self.z = ZipFS('test.zip', 'w')
    
    def tearDown(self):
        try:
            os.remove('test.zip')
        except:
            pass

    def test_binary_write_read(self):
        # GIVEN zipfs
        z = self.z

        # WHEN binary data is written to a test file in zipfs
        f = z.open('test.data', 'wb')
        f.write(self.test_content)
        f.close()
        z.close()

        # THEN the same binary data is retrieved when opened again
        z = ZipFS('test.zip', 'r')
        f = z.open('test.data', 'rb')
        content = f.read()
        f.close()
        z.close()
        self.assertEqual(content, self.test_content)

if __name__ == '__main__':
    unittest.main()
