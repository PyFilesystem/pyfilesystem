#!/usr/bin/env python
"""

  fs.tests.test_s3fs:  testcases for the S3FS module

These tests are set up to be skipped by default, since they're very slow,
require a valid AWS account, and cost money.  You'll have to set the '__test__'
attribute the True on te TestS3FS class to get them running.

"""

import unittest

from fs.tests import FSTestCases
from fs.path import *

from fs import s3fs
class TestS3FS(unittest.TestCase,FSTestCases):

    #  Disable the tests by default
    __test__ = False

    bucket = "test-s3fs.rfk.id.au"

    def setUp(self):
        self.fs = s3fs.S3FS(self.bucket)
        self._clear()

    def _clear(self):
        for (path,files) in self.fs.walk(search="depth"):
            for fn in files:
                self.fs.remove(pathjoin(path,fn))
            if path and path != "/":
                self.fs.removedir(path)

    def tearDown(self):
        self._clear()
        for k in self.fs._s3bukt.list():
            self.fs._s3bukt.delete_key(k)
        self.fs._s3conn.delete_bucket(self.bucket)



class TestS3FS_prefix(TestS3FS):

    def setUp(self):
        self.fs = s3fs.S3FS(self.bucket,"/unittest/files")
        self._clear()

