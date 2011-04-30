from fs.mountfs import MountFS
from fs.memoryfs import MemoryFS
import unittest

class TestMultiFS(unittest.TestCase):
    
    def test_auto_close(self):
        """Test MultiFS auto close is working"""        
        multi_fs = MountFS()
        m1 = MemoryFS()
        m2 = MemoryFS()
        multi_fs.mount('/m1', m1)
        multi_fs.mount('/m2', m2)
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
        multi_fs.close()
        self.assert_(m1.closed)
        self.assert_(m2.closed)
        
    def test_no_auto_close(self):
        """Test MultiFS auto close can be disabled"""
        multi_fs = MountFS(auto_close=False)
        m1 = MemoryFS()
        m2 = MemoryFS()
        multi_fs.mount('/m1', m1)
        multi_fs.mount('/m2', m2)
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
        multi_fs.close()
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
        
