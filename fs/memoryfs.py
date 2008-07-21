#!/usr/bin/env python

from fs import FS, pathsplit, _iteratepath, FSError

class MemoryFS(FS):        
    
    class DirEntry(object):                
        
        def __init__(self, type, name, contents=None):
            
            self.type = type
            self.name = name                        
            self.permissions = None
            
            if contents is None and type == "dir":
                contents = {}
                
            self.contents = contents
            
        def isdir(self):
            return self.type == "dir"

        def isfile(self):
            return self.type == "file"

    def _make_dir_entry(self, *args, **kwargs):
        
        return self.dir_entry_factory(*args, **kwargs)

    def __init__(self):
        
        self.dir_entry_factory = MemoryFS.DirEntry
        self.root = self._make_dir_entry('dir', 'root')        
        
    def _get_dir_entry(self, dirpath):
        
        current_dir = self.root
                        
        for path_component in _iteratepath(dirpath):
            dir_entry = current_dir.contents.get(path_component, None)
            if dir_entry is None:
                return None
            if not dir_entry.isdir():
                return None
            current_dir = dir_entry
            
        return current_dir
            
    def isdir(path):
        
        dir_item = self._get_dir_entry(path)
        if dir_item is None:
            return False
        return dir_item.isdir()
    
    def isfile(path):
        
        dir_item = self._get_dir_entry(path)
        if dir_item is None:
            return False
        return dir_item.isfile()
            
    def exists(path):
        
        return self._getdir(path) is not None
        
    def mkdir(self, dirname, mode=0777, recursive=False, allow_recreate=False):
        
        if not recursive:            
            dirpath, dirname = pathsplit(dirname)
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
                dir_item.contents[dirname] = self._make_dir_entry("dir", dirname)
        
        else:
            dirpath, dirname = pathsplit(dirname)
            parent_dir = self._get_dir_entry(dirpath)
            if parent_dir is not None:
                if parent_dir.isfile():
                    raise FSError("CANNOT_CREATE_DIR", "Can not create a directory, because path references a file: %(path)s", dirname)
                else:
                    if not allow_recreate:
                        raise FSError("CANNOT_RECREATE_DIR", "Can not create a directory that already exists (try allow_recreate=True): %(path)s", dirname)
            
            current_dir = self.root
            for path_component in list(_iteratepath(dirname))[:-2]:
                dir_item = current_dir.contents.get(path_component, None)
                if dir_item is None:
                    break
                if not dir_item.isdir():
                    raise FSError("CANNOT_CREATE_DIR", "Can not create a directory, because path references a file: %(path)s", dirname)
                current_dir = dir_item.contents
                
            current_dir = self.root
            for path_component in _iteratepath(dirname):
                dir_item = current_dir.contents.get(path_component, None)
                if dir_item is None:
                    new_dir = self._make_dir_entry("dir", path_component)
                    current_dir.contents[path_component] = new_dir
                    current_dir = new_dir
                
        
        return self

    
if __name__ == "__main__":
    
    mem_fs = MemoryFS()
    mem_fs.mkdir('test')
        