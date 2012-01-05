"""

  fs.tests.test_s3fs:  testcases for the S3FS module

These tests are set up to be skipped by default, since they're very slow,
require a valid AWS account, and cost money.  You'll have to set the '__test__'
attribute the True on te TestS3FS class to get them running.

"""

import unittest

from fs.tests import FSTestCases, ThreadingTestCases
from fs.path import *

from six import PY3
try:
    from fs import s3fs
except ImportError:
    raise unittest.SkipTest("s3fs wasn't importable")    
    

class TestS3FS(unittest.TestCase,FSTestCases,ThreadingTestCases):

    #  Disable the tests by default
    __test__ = False

    bucket = "test-s3fs.rfk.id.au"

    def setUp(self):        
        self.fs = s3fs.S3FS(self.bucket)
        for k in self.fs._s3bukt.list():
            self.fs._s3bukt.delete_key(k)

    def tearDown(self):
        self.fs.close()

    def test_concurrent_copydir(self):
        #  makedir() on S3FS is currently not atomic
        pass

    def test_makedir_winner(self):
        #  makedir() on S3FS is currently not atomic
        pass

    def test_multiple_overwrite(self):
        # S3's eventual-consistency seems to be breaking this test
        pass


class TestS3FS_prefix(TestS3FS):

    def setUp(self):
        self.fs = s3fs.S3FS(self.bucket,"/unittest/files")
        for k in self.fs._s3bukt.list():
            self.fs._s3bukt.delete_key(k)

    def tearDown(self):
        self.fs.close()
