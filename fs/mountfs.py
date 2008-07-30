#!/usr/bin/env python

from fs import FS, FSError, pathjoin, pathsplit, print_fs, _iteratepath
from memoryfs import MemoryFS

class MountFS(FS):
    
    class Mount(object):
        def __init__(self, path, memory_fs, value, mode):
            self.path = path
            memory_fs._on_close_memory_file(path, self)
            self.fs = None
            
        def __str__(self):
            return "Mount pont: %s, %s" % (self.path, str(self.fs))
    
    def get_mount(self, path, memory_fs, value, mode):
        
        dir_entry = memory_fs._get_dir_entry(path)
        if dir_entry is None or dir_entry.data is None:
            return MountFS.Mount(path, memory_fs, value, mode)
        else:
            return dir_entry.data
        
    
    def __init__(self):
        
        self.mounts = {}
        self.mem_fs = MemoryFS(file_factory=self.get_mount)
        
    def _delegate(self, path):
        
        path_components = list(_iteratepath(path))
                
        current_dir = self.mem_fs.root
        for i, path_component in enumerate(path_components):
            
            if current_dir is None:
                return None, None
                 
            if '.mount' in current_dir.contents:   
                break
            
            dir_entry = current_dir.contents.get(path_component, None)            
            current_dir = dir_entry
        else:
            i = len(path_components)

        if '.mount' in current_dir.contents:
                
                mount_point = '/'.join(path_components[:i])            
                mount_filename = pathjoin(mount_point, '.mount')            
                
                mount = self.mem_fs.open(mount_filename, 'r')
                delegate_path = '/'.join(path_components[i:])                
                return mount.fs, delegate_path
            
        return self, path
    
    
    def desc(self, path):
    
        fs, delegate_path = self._delegate(path)
        if fs is self:
            return "Mount dir"
        
        return "Mounted dir, maps to path %s on %s" % (delegate_path, str(fs))
        
    
    def isdir(self, path):
        
        fs, delegate_path = self._delegate(path)
        if fs is None:
            return False
        
        if fs is self:
            return True
        else:
            return fs.isdir(delegate_path)
    
    def listdir(self, path="/", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        fs, delegate_path = self._delegate(path)
                        
        if fs is None:
            raise FSError("NO_DIR", path)
        
        if fs is self:
            return self.mem_fs.listdir(path, wildcard=wildcard, full=full, absolute=absolute, hidden=hidden, dirs_only=True, files_only=False)        
        else:
            return fs.listdir(delegate_path, wildcard=wildcard, full=full, absolute=absolute, hidden=hidden, dirs_only=dirs_only, files_only=files_only)
        
    
    def mount(self, name, path, fs):
                
        self.mem_fs.mkdir(path, recursive=True)
        mount_filename = pathjoin(path, '.mount')
        mount = self.mem_fs.open(mount_filename, 'w')
        mount.name = name
        mount.fs = fs        
        
        self.mounts[name] = (path, fs)

if __name__ == "__main__":
    
    fs1 = MemoryFS()
    fs1.mkdir("Memroot/B/C/D", recursive=True)
    fs1.open("test.txt", 'w').write("Hello, World!")
    
    #print_fs(fs1)
    
    mountfs = MountFS()
    mountfs.mount("fs1", '1/2', fs1)
    mountfs.mount("fs1", '1/another', fs1)
    
    #print mountfs.listdir('1/2/Memroot/B/C')
    
    print mountfs.desc('1/2/Memroot/B')
    print_fs(mountfs)
    
    #print mountfs._delegate('1/2/Memroot/B')