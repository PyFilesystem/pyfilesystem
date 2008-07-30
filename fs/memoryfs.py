#!/usr/bin/env python

import os
import datetime
from fs import FS, pathsplit, _iteratepath, FSError, print_fs

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

def _check_mode(mode, mode_chars):
    for c in mode_chars:
        if c not in mode:
            return False
    return True

class MemoryFile(object):

    def __init__(self, path, memory_fs, value, mode):

        self.path = path
        self.memory_fs = memory_fs
        self.mode = mode

        self.mem_file = None

        if _check_mode(mode, 'wa'):
            self.mem_file = StringIO()
            self.mem_file.write(value)

        elif _check_mode(mode, 'w'):

            self.mem_file = StringIO()

        elif _check_mode(mode, 'ra'):

            self.mem_file = StringIO()
            self.mem_file.write(value)

        elif _check_mode(mode, 'r'):

            self.mem_file = StringIO(value)

        else:

            if value is not None:
                self.mem_file = StringIO(value)
            else:
                self.mem_file = StringIO()

        assert self.mem_file is not None, "self.mem_file should have a value"

        self.closed = False

    def __del__(self):

        if not self.closed:
            self.close()


    def flush(self):
        pass

    def __iter__(self):
        return iter(self.mem_file)

    def next(self):
        return self.mem_file.next()

    def readline(self, *args, **kwargs):
        return self.mem_file.readline(*args, **kwargs)

    def close(self):
        if not self.closed:
            value = self.mem_file.getvalue()
            self.memory_fs._on_close_memory_file(self.path, value)
            self.mem_file.close()
            self.closed = True

    def read(self, size=None):
        if size is None:
            size = -1
        return self.mem_file.read(size)

    def seek(self, *args, **kwargs):
        return self.mem_file.seek(*args, **kwargs)

    def tell(self):
        return self.mem_file.tell()

    def truncate(self, *args, **kwargs):
        return self.mem_file.truncate(*args, **kwargs)

    def write(self, data):
        return self.mem_file.write(data)

    def writelines(self, *args, **kwargs):
        return self.mem_file.writelines(*args, **kwargs)




class MemoryFS(FS):

    class DirEntry(object):

        def __init__(self, type, name, contents=None):

            self.type = type
            self.name = name
            self.permissions = None

            if contents is None and type == "dir":
                contents = {}

            self.contents = contents

            self.data = None

            self.locks = 0

            self.created_time = datetime.datetime.now()

        def lock(self):
            self.locks += 1

        def unlock(self):
            self.locks -=1
            assert self.locks >=0, "Lock / Unlock mismatch!"

        def desc_contents(self):
            if self.isfile():
                return "<file %s>"%self.name
            elif self.isdir():
                return "<dir %s>"%"".join( "%s: %s"% (k, v.desc_contents()) for k, v in self.contents.iteritems())

        def isdir(self):
            return self.type == "dir"

        def isfile(self):
            return self.type == "file"

        def islocked(self):
            return self.locks > 0

        def __str__(self):
            return "%s: %s" % (self.name, self.desc_contents())

    def _make_dir_entry(self, *args, **kwargs):

        return self.dir_entry_factory(*args, **kwargs)

    def __init__(self, file_factory=None):

        self.dir_entry_factory = MemoryFS.DirEntry
        self.file_factory = file_factory or MemoryFile
            
        self.root = self._make_dir_entry('dir', 'root')        

    def __str__(self):
        return "<MemoryFS>"

    def _get_dir_entry(self, dirpath):

        current_dir = self.root

        for path_component in _iteratepath(dirpath):
            dir_entry = current_dir.contents.get(path_component, None)
            if dir_entry is None:
                return None
            current_dir = dir_entry

        return current_dir

    def getsyspath(self, pathname):

        raise FSError("NO_SYS_PATH", pathname, msg="This file-system has no syspath")

    def desc(self, path):
        
        if self.isdir(path):
            return "Memory dir"
        elif self.isfile(path):
            return "Memory file object"
        else:
            return "No description available"

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

        return self._get_dir_entry(path) is not None

    def mkdir(self, dirname, mode=0777, recursive=False, allow_recreate=False):

        fullpath = dirname
        dirpath, dirname = pathsplit(dirname)

        if recursive:
            parent_dir = self._get_dir_entry(dirpath)
            if parent_dir is not None:
                if parent_dir.isfile():
                    raise FSError("NO_DIR", dirname, msg="Can not create a directory, because path references a file: %(path)s")
                else:
                    if not allow_recreate:
                        if dirname in parent_dir.contents:
                            raise FSError("NO_DIR", dirname, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")

            current_dir = self.root
            for path_component in _iteratepath(dirpath)[:-1]:
                dir_item = current_dir.contents.get(path_component, None)
                if dir_item is None:
                    break
                if not dir_item.isdir():
                    raise FSError("NO_DIR", dirname, msg="Can not create a directory, because path references a file: %(path)s")
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
                raise FSError("NO_DIR", dirname, msg="Could not make dir, as parent dir does not exist: %(path)s")

        dir_item = parent_dir.contents.get(dirname, None)
        if dir_item is not None:
            if dir_item.isdir():
                if not allow_recreate:
                    raise FSError("DIR_EXISTS", dirname)
            else:
                raise FSError("NO_DIR", dirname, msg="Can not create a directory, because path references a file: %(path)s")

        if dir_item is None:
            parent_dir.contents[dirname] = self._make_dir_entry("dir", dirname)

        return self


    def _lock_dir_entry(self, path):

        dir_entry = self._get_dir_entry(path)
        dir_entry.lock()


    def _unlock_dir_entry(self, path):

        dir_entry = self._get_dir_entry(path)
        dir_entry.unlock()

    def _is_dir_locked(self, path):

        dir_entry = self._get_dir_entry(path)
        return dir_entry.islocked()


    def open(self, path, mode="r", **kwargs):

        filepath, filename = pathsplit(path)
        parent_dir_entry = self._get_dir_entry(filepath)

        if parent_dir_entry is None or not parent_dir_entry.isdir():
            raise FSError("NO_FILE", path)

        if 'r' in mode or 'a' in mode:
            if filename not in parent_dir_entry.contents:
                raise FSError("NO_FILE", path)

            file_dir_entry = parent_dir_entry.contents[filename]

            if 'a' in mode and  file_dir_entry.islocked():
                raise FSError("FILE_LOCKED", path)

            self._lock_dir_entry(path)
            mem_file = self.file_factory(path, self, file_dir_entry.data, mode)
            return mem_file

        elif 'w' in mode:


            if filename not in parent_dir_entry.contents:
                file_dir_entry = self._make_dir_entry("file", filename)
                parent_dir_entry.contents[filename] = file_dir_entry
            else:
                file_dir_entry = parent_dir_entry.contents[filename]

            if file_dir_entry.islocked():
                raise FSError("FILE_LOCKED", path)

            self._lock_dir_entry(path)

            mem_file = self.file_factory(path, self, None, mode)
            return mem_file

        if parent_dir_entry is None:
            raise FSError("NO_FILE", path)


    def remove(self, path):

        dir_entry = self._get_dir_entry(path)

        if dir_entry is None:
            raise FSError("NO_FILE", path)

        if dir_entry.islocked():
            raise FSError("FILE_LOCKED", path)

        pathname, dirname = pathsplit(path)

        parent_dir = self._get_dir_entry(pathname)

        del parent_dir.contents[dirname]




    def _on_close_memory_file(self, path, value):

        filepath, filename = pathsplit(path)
        dir_entry = self._get_dir_entry(path)
        dir_entry.data = value
        self._unlock_dir_entry(path)



    def listdir(self, path="/", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        dir_entry = self._get_dir_entry(path)
        if dir_entry is None:
            raise FSError("NO_DIR", path)
        paths = dir_entry.contents.keys()

        return self._listdir_helper(path, paths, wildcard, full, absolute, hidden, dirs_only, files_only)

    def getinfo(self, path):

        dir_entry = self._get_dir_entry(path)

        info = {}
        info['created_time'] = dir_entry.created_time

        if dir_entry.isfile():
            info['size'] = len(dir_entry.data)

        return info


    def ishidden(self, pathname):
        return False



def main():

    mem_fs = MemoryFS()
    mem_fs.mkdir('test/test2', recursive=True)
    mem_fs.mkdir('test/A', recursive=True)
    mem_fs.mkdir('test/A/B', recursive=True)



    mem_fs.open("test/readme.txt", 'w').write("Hello, World!")

    mem_fs.open("test/readme.txt", 'wa').write("\nSecond Line")

    print mem_fs.open("test/readme.txt", 'r').read()


    f1 = mem_fs.open("/test/readme.txt", 'r')
    f2 = mem_fs.open("/test/readme.txt", 'r')
    print f1.read(10)
    print f2.read(10)
    f1.close()
    f2.close()
    f3 = mem_fs.open("/test/readme.txt", 'w')



    #print mem_fs.listdir('test')
    #print mem_fs.isdir("test/test2")
    #print mem_fs.root
    print_fs(mem_fs)

    from browsewin import browse
    browse(mem_fs)


if __name__ == "__main__":

    main()
