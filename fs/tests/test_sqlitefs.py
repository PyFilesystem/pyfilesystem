try:
    from fs.contrib.sqlitefs import SqliteFS
except ImportError:
    SqliteFS = None
from fs.tests import FSTestCases
import unittest

import os

if SqliteFS:
    class TestSqliteFS(unittest.TestCase, FSTestCases):
        def setUp(self):
            self.fs = SqliteFS("sqlitefs.db")            
        def tearDown(self):
            os.remove('sqlitefs.db')
        
        