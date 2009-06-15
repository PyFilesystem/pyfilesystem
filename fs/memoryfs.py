#!/usr/bin/env python
"""

  fs.memoryfs:  A filesystem that exists only in memory

Obviously that makes this particular filesystem very fast...

"""

import datetime
from fs.path import iteratepath
from fs.base import *

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
        self.closed = False
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

        elif _check_mode(mode, "a"):
            self.mem_file = StringIO()
            self.mem_file.write(value)

        else:
            if value is not None:
                self.mem_file = StringIO(value)
            else:
                self.mem_file = StringIO()

        assert self.mem_file is not None, "self.mem_file should have a value"

    def __str__(self):
        return "<MemoryFile in %s %s>" % (self.memory_fs, self.path)

    __repr__ = __str__

    def __unicode__(self):
        return unicode(self.__str__())

    def __del__(self):
        if not self.closed:
            self.close()

    def flush(self):
        value = self.mem_file.getvalue()
        self.memory_fs._on_flush_memory_file(self.path, value)

    def __iter__(self):
        return iter(self.mem_file)

    def next(self):
        return self.mem_file.next()

    def readline(self, *args, **kwargs):
        return self.mem_file.readline(*args, **kwargs)

    def close(self):
        if not self.closed and self.mem_file is not None:
            value = self.mem_file.getvalue()
            self.memory_fs._on_close_memory_file(self, self.path, value)
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

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_value,traceback):
        self.close()
        return False



class DirEntry(object):

    def __init__(self, type, name, contents=None):

        assert type in ("dir", "file"), "Type must be dir or file!"

        self.type = type
        self.name = name

        if contents is None and type == "dir":
            contents = {}

        self.open_files = []
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


class MemoryFS(FS):

    def _make_dir_entry(self, *args, **kwargs):
        return self.dir_entry_factory(*args, **kwargs)

    def __init__(self, file_factory=None):
        FS.__init__(self, thread_synchronize=True)
        self.dir_entry_factory = DirEntry
        self.file_factory = file_factory or MemoryFile

        self.root = self._make_dir_entry('dir', 'root')

    def __str__(self):
        return "<MemoryFS>"

    __repr__ = __str__

    def __unicode__(self):
        return unicode(self.__str__())

    @synchronize
    def _get_dir_entry(self, dirpath):
        current_dir = self.root
        for path_component in iteratepath(dirpath):
            if current_dir.contents is None:
                return None
            dir_entry = current_dir.contents.get(path_component, None)
            if dir_entry is None:
                return None
            current_dir = dir_entry
        return current_dir

    @synchronize
    def desc(self, path):
        if self.isdir(path):
            return "Memory dir"
        elif self.isfile(path):
            return "Memory file object"
        else:
            return "No description available"

    @synchronize
    def isdir(self, path):
        dir_item = self._get_dir_entry(normpath(path))
        if dir_item is None:
            return False
        return dir_item.isdir()

    @synchronize
    def isfile(self, path):
        dir_item = self._get_dir_entry(normpath(path))
        if dir_item is None:
            return False
        return dir_item.isfile()

    @synchronize
    def exists(self, path):
        return self._get_dir_entry(path) is not None

    @synchronize
    def makedir(self, dirname, recursive=False, allow_recreate=False):
        if not dirname:
            raise PathError("", "Path is empty")
        fullpath = dirname
        dirpath, dirname = pathsplit(dirname)

        if recursive:
            parent_dir = self._get_dir_entry(dirpath)
            if parent_dir is not None:
                if parent_dir.isfile():
                    raise ResourceInvalidError(dirname, msg="Can not create a directory, because path references a file: %(path)s")
                else:
                    if not allow_recreate:
                        if dirname in parent_dir.contents:
                            raise DestinationExistsError(dirname, msg="Can not create a directory that already exists (try allow_recreate=True): %(path)s")

            current_dir = self.root
            for path_component in iteratepath(dirpath)[:-1]:
                dir_item = current_dir.contents.get(path_component, None)
                if dir_item is None:
                    break
                if not dir_item.isdir():
                    raise ResourceInvalidError(dirname, msg="Can not create a directory, because path references a file: %(path)s")
                current_dir = dir_item

            current_dir = self.root
            for path_component in iteratepath(dirpath):
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
                raise ParentDirectoryMissingError(dirname, msg="Could not make dir, as parent dir does not exist: %(path)s")

        dir_item = parent_dir.contents.get(dirname, None)
        if dir_item is not None:
            if dir_item.isdir():
                if not allow_recreate:
                    raise DestinationExistsError(dirname)
            else:
                raise ResourceInvalidError(dirname, msg="Can not create a directory, because path references a file: %(path)s")

        if dir_item is None:
            parent_dir.contents[dirname] = self._make_dir_entry("dir", dirname)

        return self

    def _orphan_files(self, file_dir_entry):
        for f in file_dir_entry.open_files:
            f.close()

    @synchronize
    def _lock_dir_entry(self, path):
        dir_entry = self._get_dir_entry(path)
        dir_entry.lock()

    @synchronize
    def _unlock_dir_entry(self, path):
        dir_entry = self._get_dir_entry(path)
        dir_entry.unlock()

    @synchronize
    def _is_dir_locked(self, path):
        dir_entry = self._get_dir_entry(path)
        return dir_entry.islocked()

    @synchronize
    def open(self, path, mode="r", **kwargs):
        filepath, filename = pathsplit(path)
        parent_dir_entry = self._get_dir_entry(filepath)

        if parent_dir_entry is None or not parent_dir_entry.isdir():
            raise ResourceNotFoundError(path)

        if 'r' in mode or 'a' in mode:
            if filename not in parent_dir_entry.contents:
                raise ResourceNotFoundError(path)

            file_dir_entry = parent_dir_entry.contents[filename]

            if 'a' in mode and  file_dir_entry.islocked():
                raise ResourceLockedError(path)

            self._lock_dir_entry(path)
            mem_file = self.file_factory(path, self, file_dir_entry.data, mode)
            file_dir_entry.open_files.append(mem_file)
            return mem_file

        elif 'w' in mode:
            if filename not in parent_dir_entry.contents:
                file_dir_entry = self._make_dir_entry("file", filename)
                parent_dir_entry.contents[filename] = file_dir_entry
            else:
                file_dir_entry = parent_dir_entry.contents[filename]

            if file_dir_entry.islocked():
                raise ResourceLockedError(path)

            self._lock_dir_entry(path)

            mem_file = self.file_factory(path, self, None, mode)
            file_dir_entry.open_files.append(mem_file)
            return mem_file

        if parent_dir_entry is None:
            raise ResourceNotFoundError(path)

    @synchronize
    def remove(self, path):
        dir_entry = self._get_dir_entry(path)

        if dir_entry is None:
            raise ResourceNotFoundError(path)

        if dir_entry.islocked():
            self._orphan_files(dir_entry)
            #raise ResourceLockedError("FILE_LOCKED", path)

        if dir_entry.isdir():
            raise ResourceInvalidError(path,msg="That's a directory, not a file: %(path)s")

        pathname, dirname = pathsplit(path)

        parent_dir = self._get_dir_entry(pathname)

        del parent_dir.contents[dirname]

    @synchronize
    def removedir(self, path, recursive=False, force=False):
        dir_entry = self._get_dir_entry(path)

        if dir_entry is None:
            raise ResourceNotFoundError(path)
        if dir_entry.islocked():
            raise ResourceLockedError(path)
        if not dir_entry.isdir():
            raise ResourceInvalidError(path, msg="Can't remove resource, its not a directory: %(path)s" )

        if dir_entry.contents and not force:
            raise DirectoryNotEmptyError(path)

        if recursive:
            rpathname = path
            while rpathname:
                rpathname, dirname = pathsplit(rpathname)
                parent_dir = self._get_dir_entry(rpathname)
                del parent_dir.contents[dirname]
        else:
            pathname, dirname = pathsplit(path)
            parent_dir = self._get_dir_entry(pathname)
            del parent_dir.contents[dirname]


    @synchronize
    def rename(self, src, dst):
        if not issamedir(src, dst):
            raise ValueError("Destination path must the same directory (use the move method for moving to a different directory)")

        dst = pathsplit(dst)[-1]

        dir_entry = self._get_dir_entry(src)
        if dir_entry is None:
            raise ResourceNotFoundError(src)
        #if dir_entry.islocked():
        #    raise ResourceLockedError("FILE_LOCKED", src)

        open_files = dir_entry.open_files[:]
        for f in open_files:
            f.flush()
            f.path = dst

        dst_dir_entry = self._get_dir_entry(dst)
        if dst_dir_entry is not None:
            raise DestinationExistsError(path)

        pathname, dirname = pathsplit(src)
        parent_dir = self._get_dir_entry(pathname)
        parent_dir.contents[dst] = parent_dir.contents[dirname]
        parent_dir.name = dst
        del parent_dir.contents[dirname]


    @synchronize
    def _on_close_memory_file(self, open_file, path, value):
        filepath, filename = pathsplit(path)
        dir_entry = self._get_dir_entry(path)
        if dir_entry is not None and value is not None:
            dir_entry.data = value
            dir_entry.open_files.remove(open_file)
            self._unlock_dir_entry(path)

    @synchronize
    def _on_flush_memory_file(self, path, value):
        filepath, filename = pathsplit(path)
        dir_entry = self._get_dir_entry(path)
        dir_entry.data = value

    @synchronize
    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        dir_entry = self._get_dir_entry(path)
        if dir_entry is None:
            raise ResourceNotFoundError(path)
        if dir_entry.isfile():
            raise ResourceInvalidError(path,msg="that's a file, not a directory: %(path)s")
        paths = dir_entry.contents.keys()
        return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)

    @synchronize
    def getinfo(self, path):
        dir_entry = self._get_dir_entry(path)

        if dir_entry is None:
            raise ResourceNotFoundError(path)

        info = {}
        info['created_time'] = dir_entry.created_time

        if dir_entry.isfile():
            info['size'] = len(dir_entry.data or '')

        return info


