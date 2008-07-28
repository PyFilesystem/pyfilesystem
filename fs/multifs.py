#!/usr/bin/env python

from fs import FS

class MultiFS(FS):
    
    def __init__(self):
        FS.__init__(self)
        
        self.fs_sequence = []
        self.fs_lookup =  {}
        

    def add_fs(self, name, fs):
                
        self.fs_sequence.append(name, fs)
        self.fs_lookup[name] = fs
        
        
    def remove_fs(self, name):
        
        fs = self.fs_lookup[name]
        self.fs_sequence.remove(fs)
        del self.fs_lookup[name]
        
        
        
    def __getitem__(self, name):
        
        return self.fs_lookup[name]
    
    def __iter__(self):
        
        return iter(self.fs_sequence)
    
    def _delegate_search(self, path):
        
        for fs in self:
            if self.exists(path):
                return fs
        return None
    
    def exists(self, path):
        
        return self._delegate_search(path) is not None
            