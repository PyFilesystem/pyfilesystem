"""

    Test the walk function and related code

"""
import unittest
from fs.memoryfs import MemoryFS

class TestWalk(unittest.TestCase):
    
    def setUp(self):
        self.fs = MemoryFS()
        self.fs.createfile('a.txt', 'hello')
        self.fs.createfile('b.txt', 'world')
        self.fs.makeopendir('foo').createfile('c', '123')
        self.fs.makeopendir('.svn').createfile('ignored')
    
    def test_wildcard(self):
        for dir_path, paths in self.fs.walk(wildcard='*.txt'):
            for path in paths:
                self.assert_(path.endswith('.txt'))
        for dir_path, paths in self.fs.walk(wildcard=lambda fn:fn.endswith('.txt')):
            for path in paths:
                self.assert_(path.endswith('.txt'))
    
    def test_dir_wildcard(self):
        
        for dir_path, paths in self.fs.walk(dir_wildcard=lambda fn:not fn.endswith('.svn')):            
            for path in paths:                
                self.assert_('.svn' not in path)
        