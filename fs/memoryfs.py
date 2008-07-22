#!/usr/bin/env python

from fs import FS, pathsplit, _iteratepath, FSError, print_fs

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

class MemoryFile(StringIO):
    
    def __init__(self, path, memory_fs, *args, **kwargs):
        
        self.path = path
        self.memory_fs = memory_fs
        
        StringIO.__init__(*args, **kwargs)
        
    def close():
        
        value = self.get_vale()
        self.memory_fs._on_close_memory_file(path, value)
        StringIO.close(self)
        
    
    

class MemoryFS(FS):        
    
    class DirEntry(object):                
        
        def __init__(self, type, name, contents=None):
            
            self.type = type
            self.name = name                        
            self.permissions = None
            
            if contents is None and type == "dir":
                contents = {}
                
            self.contents = contents
            
        def desc_contents(self):
            if self.isfile():
                return "<file>"
            elif self.isdir():
                return "<dir %s>"%"".join( "%s: %s"% (k, v.desc_contents()) for k, v in self.contents.iteritems())
            
        def isdir(self):
            return self.type == "dir"

        def isfile(self):
            return self.type == "file"
        
        def __str__(self):
            return "%s: %s" % (self.name, self.desc_contents())
        
    class FileEntry(object):
        
        def __init__(self):
            self.memory_file = None
            self.value = ""

    def _make_dir_entry(self, *args, **kwargs):
        
        return self.dir_entry_factory(*args, **kwargs)

    def __init__(self):
        
        self.dir_entry_factory = MemoryFS.DirEntry
        self.root = self._make_dir_entry('dir', 'root')        
        
    def _get_dir_entry(self, dirpath):
                
        current_dir = self.root
        
        #print _iteratepath(dirpath)        
        for path_component in _iteratepath(dirpath):
            dir_entry = current_dir.contents.get(path_component, None)
            if dir_entry is None:
                return None
            if not dir_entry.isdir():
                return None
            current_dir = dir_entry
            
        return current_dir
    
    def getsyspath(self, pathname):
        
        raise FSError("NO_SYS_PATH", "This file-system has no syspath", pathname)
    
            
    def isdir(self, path):
        
        dir_item = self._get_dir_entry(self._resolve(path))
        if dir_item is None:            
            return False
        return dir_item.isdir()
    
    def isfile(self, path):
        
        dir_item = self._get_dir_entry(self._resolve(path))
        if dir_item is None:
            return False
        return dir_item.isfile()
            
    def exists(self, path):
        
        return self._getdir(path) is not None
        
    def mkdir(self, dirname, mode=0777, recursive=False, allow_recreate=False):
        
        fullpath = dirname
        dirpath, dirname = pathsplit(dirname)            
        
        if recursive:            
            parent_dir = self._get_dir_entry(dirpath)
            if parent_dir is not None:
                if parent_dir.isfile():                    
                    raise FSError("CANNOT_CREATE_DIR", "Can not create a directory, because path references a file: %(path)s", dirname)
                else:                    
                    if not allow_recreate:
                        if dirname in parent_dir.contents:
                            raise FSError("CANNOT_RECREATE_DIR", "Can not create a directory that already exists (try allow_recreate=True): %(path)s", dirname)
            
            current_dir = self.root
            for path_component in _iteratepath(dirpath)[:-1]:
                dir_item = current_dir.contents.get(path_component, None)
                if dir_item is None:
                    break
                if not dir_item.isdir():
                    raise FSError("CANNOT_CREATE_DIR", "Can not create a directory, because path references a file: %(path)s", dirname)
                current_dir = dir_item.contents
                
            current_dir = self.root            
            for path_component in _iteratepath(dirpath):
                dir_item = current_dir.contents.get(path_component, None)
                if dir_item is None:
                    new_dir = self._make_dir_entry("dir", path_component)
                    current_dir.contents[path_component] = new_dir
                    current_dir = new_dir
                else:
                    current_dir = dir_item
                    
            parent_dir = current_dir            
            
        else:                
            parent_dir = self._get_dir_entry(dirpath)            
            if parent_dir is None:
                raise FSError("NO_DIR", "Could not make dir, as parent dir does not exist: %(path)s", dirname )
        
        dir_item = parent_dir.contents.get(dirname, None)        
        if dir_item is not None:            
            if dir_item.isdir():
                if not allow_recreate:
                    raise FSError("CANNOT_RECREATE_DIR", "Can not create a directory that already exists (try allow_recreate=True): %(path)s", dirname)
            else:
                raise FSError("CANNOT_CREATE_DIR", "Can not create a directory, because path references a file: %(path)s", dirname)
        
        if dir_item is None:
            parent_dir.contents[dirname] = self._make_dir_entry("dir", dirname)
        
        return self
    
    def open(self, path, mode, **kwargs):
                
        dir_entry = self._get_dir_entry(path)
        if dir_entry is None:
            dirpath, dirname = pathsplit(path)
        parent_dir_entry = self._get_dir_entry(dirpath)
        
        
        
        if parent_dir_entry is None:
            raise FSError("DOES_NOT_EXIST", "File does not exist", path)
        
    def _on_close_memory_file(self, path, value):
        
        dir_entry = self._get_dir_entry(path)
        
    
    def listdir(self, path="/", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        dir_entry = self._get_dir_entry(path)
        paths = dir_entry.contents.keys()        

        return self._listdir_helper(path, paths, wildcard, full, absolute, hidden, dirs_only, files_only)


    def ishidden(self, pathname):
        return False
    
if __name__ == "__main__":
    
    mem_fs = MemoryFS()
    mem_fs.mkdir('test/test2', recursive=True)
    mem_fs.mkdir('test/A', recursive=True)
    mem_fs.mkdir('test/A/B', recursive=True)
    #print mem_fs.listdir('test')
    #print mem_fs.isdir("test/test2")
    #print mem_fs.root
    print_fs(mem_fs)
        