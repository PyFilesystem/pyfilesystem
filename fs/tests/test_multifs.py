from fs.multifs import MultiFS
from fs.memoryfs import MemoryFS
import unittest

from six import b

class TestMultiFS(unittest.TestCase):
    
    def test_auto_close(self):
        """Test MultiFS auto close is working"""       
        multi_fs = MultiFS()
        m1 = MemoryFS()
        m2 = MemoryFS()
        multi_fs.addfs('m1', m1)
        multi_fs.addfs('m2', m2)
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
        multi_fs.close()
        self.assert_(m1.closed)
        self.assert_(m2.closed)
        
    def test_no_auto_close(self):
        """Test MultiFS auto close can be disables"""
        multi_fs = MultiFS(auto_close=False)
        m1 = MemoryFS()
        m2 = MemoryFS()
        multi_fs.addfs('m1', m1)
        multi_fs.addfs('m2', m2)
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
        multi_fs.close()
        self.assert_(not m1.closed)
        self.assert_(not m2.closed)
    
    
    def test_priority(self):
        """Test priority order is working"""
        m1 = MemoryFS()
        m2 = MemoryFS()
        m3 = MemoryFS()
        m1.setcontents("name", b("m1"))
        m2.setcontents("name", b("m2"))
        m3.setcontents("name", b("m3"))
        multi_fs = MultiFS(auto_close=False)
        multi_fs.addfs("m1", m1)
        multi_fs.addfs("m2", m2)
        multi_fs.addfs("m3", m3)
        self.assert_(multi_fs.getcontents("name") == b("m3"))
        
        m1 = MemoryFS()
        m2 = MemoryFS()
        m3 = MemoryFS()
        m1.setcontents("name", b("m1"))
        m2.setcontents("name", b("m2"))
        m3.setcontents("name", b("m3"))
        multi_fs = MultiFS(auto_close=False)
        multi_fs.addfs("m1", m1)
        multi_fs.addfs("m2", m2, priority=10)
        multi_fs.addfs("m3", m3)
        self.assert_(multi_fs.getcontents("name") == b("m2"))        
        
        m1 = MemoryFS()
        m2 = MemoryFS()
        m3 = MemoryFS()
        m1.setcontents("name", b("m1"))
        m2.setcontents("name", b("m2"))
        m3.setcontents("name", b("m3"))
        multi_fs = MultiFS(auto_close=False)
        multi_fs.addfs("m1", m1)
        multi_fs.addfs("m2", m2, priority=10)
        multi_fs.addfs("m3", m3, priority=10)
        self.assert_(multi_fs.getcontents("name") == b("m3"))
        
        m1 = MemoryFS()
        m2 = MemoryFS()
        m3 = MemoryFS()
        m1.setcontents("name", b("m1"))
        m2.setcontents("name", b("m2"))
        m3.setcontents("name", b("m3"))
        multi_fs = MultiFS(auto_close=False)
        multi_fs.addfs("m1", m1, priority=11)
        multi_fs.addfs("m2", m2, priority=10)
        multi_fs.addfs("m3", m3, priority=10)
        self.assert_(multi_fs.getcontents("name") == b("m1"))
        
