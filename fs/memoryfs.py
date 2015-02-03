#!/usr/bin/env python
"""
fs.memoryfs
===========

A Filesystem that exists in memory only. Which makes them extremely fast, but non-permanent.

If you open a file from a `memoryfs` you will get back a StringIO object from the standard library.


"""

import datetime
import stat
from fs.path import iteratepath, pathsplit, normpath
from fs.base import *
from fs.errors import *
from fs import _thread_synchronize_default
from fs.filelike import StringIO
from fs import iotools
from os import SEEK_END
import threading

import six
from six import b


def _check_mode(mode, mode_chars):
    for c in mode_chars:
        if c not in mode:
            return False
    return True


class MemoryFile(object):

    def seek_and_lock(f):
        def deco(self, *args, **kwargs):
            try:
                self._lock.acquire()
                self.mem_file.seek(self.pos)
                ret = f(self, *args, **kwargs)
                self.pos = self.mem_file.tell()
                return ret
            finally:
                self._lock.release()
        return deco

    def __init__(self, path, memory_fs, mem_file, mode, lock):
        self.closed = False
        self.path = path
        self.memory_fs = memory_fs
        self.mem_file = mem_file
        self.mode = mode
        self._lock = lock

        self.pos = 0

        if _check_mode(mode, 'a'):
            lock.acquire()
            try:
                self.mem_file.seek(0, SEEK_END)
                self.pos = self.mem_file.tell()
            finally:
                lock.release()

        elif _check_mode(mode, 'w'):
            lock.acquire()
            try:
                self.mem_file.seek(0)
                self.mem_file.truncate()
            finally:
                lock.release()

        assert self.mem_file is not None, "self.mem_file should have a value"

    def __str__(self):
        return "<MemoryFile in %s %s>" % (self.memory_fs, self.path)

    def __repr__(self):
        return u"<MemoryFile in %s %s>" % (self.memory_fs, self.path)

    def __unicode__(self):
        return u"<MemoryFile in %s %s>" % (self.memory_fs, self.path)

    def __del__(self):
        if not self.closed:
            self.close()

    def flush(self):
        pass

    def __iter__(self):
        if 'r' not in self.mode and '+' not in self.mode:
            raise IOError("File not open for reading")
        self.mem_file.seek(self.pos)
        for line in self.mem_file:
            yield line

    @seek_and_lock
    def next(self):
        if 'r' not in self.mode and '+' not in self.mode:
            raise IOError("File not open for reading")
        return self.mem_file.next()

    @seek_and_lock
    def readline(self, *args, **kwargs):
        if 'r' not in self.mode and '+' not in self.mode:
            raise IOError("File not open for reading")
        return self.mem_file.readline(*args, **kwargs)

    def close(self):
        do_close = False
        self._lock.acquire()
        try:
            do_close = not self.closed and self.mem_file is not None
            if do_close:
                self.closed = True
        finally:
            self._lock.release()
        if do_close:
            self.memory_fs._on_close_memory_file(self, self.path)

    @seek_and_lock
    def read(self, size=None):
        if 'r' not in self.mode and '+' not in self.mode:
            raise IOError("File not open for reading")
        if size is None:
            size = -1
        return self.mem_file.read(size)

    @seek_and_lock
    def seek(self, *args, **kwargs):
        return self.mem_file.seek(*args, **kwargs)

    @seek_and_lock
    def tell(self):
        return self.pos

    @seek_and_lock
    def truncate(self, *args, **kwargs):
        if 'r' in self.mode and '+' not in self.mode:
            raise IOError("File not open for writing")
        return self.mem_file.truncate(*args, **kwargs)

    #@seek_and_lock
    def write(self, data):
        if 'r' in self.mode and '+' not in self.mode:
            raise IOError("File not open for writing")
        self.memory_fs._on_modify_memory_file(self.path)
        self._lock.acquire()
        try:
            self.mem_file.seek(self.pos)
            self.mem_file.write(data)
            self.pos = self.mem_file.tell()
        finally:
            self._lock.release()

    @seek_and_lock
    def writelines(self, *args, **kwargs):
        return self.mem_file.writelines(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False


class DirEntry(object):

    def sync(f):
        def deco(self, *args, **kwargs):
            if self.lock is not None:
                try:
                    self.lock.acquire()
                    return f(self, *args, **kwargs)
                finally:
                    self.lock.release()
            else:
                return f(self, *args, **kwargs)
        return deco

    def __init__(self, type, name, contents=None):

        assert type in ("dir", "file"), "Type must be dir or file!"

        self.type = type
        self.name = name

        if contents is None and type == "dir":
            contents = {}

        self.open_files = []
        self.contents = contents
        self.mem_file = None
        self.created_time = datetime.datetime.now()
        self.modified_time = self.created_time
        self.accessed_time = self.created_time

        self.xattrs = {}

        self.lock = None
        if self.type == 'file':
            self.mem_file = StringIO()
            self.lock = threading.RLock()

    def get_value(self):
        self.lock.acquire()
        try:
            return self.mem_file.getvalue()
        finally:
            self.lock.release()
    data = property(get_value)

    def desc_contents(self):
        if self.isfile():
            return "<file %s>" % self.name
        elif self.isdir():
            return "<dir %s>" % "".join("%s: %s" % (k, v.desc_contents()) for k, v in self.contents.iteritems())

    def isdir(self):
        return self.type == "dir"

    def isfile(self):
        return self.type == "file"

    def __str__(self):
        return "%s: %s" % (self.name, self.desc_contents())

    @sync
    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('lock')
        if self.mem_file is not None:
            state['mem_file'] = self.data
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if self.type == 'file':
            self.lock = threading.RLock()
        else:
            self.lock = None
        if self.mem_file is not None:
            data = self.mem_file
            self.mem_file = StringIO()
            self.mem_file.write(data)


class MemoryFS(FS):
    """An in-memory filesystem.

    """

    _meta = {'thread_safe': True,
             'network': False,
             'virtual': False,
             'read_only': False,
             'unicode_paths': True,
             'case_insensitive_paths': False,
             'atomic.move': False,
             'atomic.copy': False,
             'atomic.makedir': True,
             'atomic.rename': True,
             'atomic.setcontents': False}

    def _make_dir_entry(self, *args, **kwargs):
        return self.dir_entry_factory(*args, **kwargs)

    def __init__(self, file_factory=None):
        super(MemoryFS, self).__init__(thread_synchronize=_thread_synchronize_default)

        self.dir_entry_factory = DirEntry
        self.file_factory = file_factory or MemoryFile
        if not callable(self.file_factory):
            raise ValueError("file_factory should be callable")

        self.root = self._make_dir_entry('dir', 'root')

    def __str__(self):
        return "<MemoryFS>"

    def __repr__(self):
        return "MemoryFS()"

    def __unicode__(self):
        return "<MemoryFS>"

    @synchronize
    def _get_dir_entry(self, dirpath):
        dirpath = normpath(dirpath)
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
    def _dir_entry(self, path):
        dir_entry = self._get_dir_entry(path)
        if dir_entry is None:
            raise ResourceNotFoundError(path)
        return dir_entry

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
        path = normpath(path)
        if path in ('', '/'):
            return True
        dir_item = self._get_dir_entry(path)
        if dir_item is None:
            return False
        return dir_item.isdir()

    @synchronize
    def isfile(self, path):
        path = normpath(path)
        if path in ('', '/'):
            return False
        dir_item = self._get_dir_entry(path)
        if dir_item is None:
            return False
        return dir_item.isfile()

    @synchronize
    def exists(self, path):
        path = normpath(path)
        if path in ('', '/'):
            return True
        return self._get_dir_entry(path) is not None

    @synchronize
    def makedir(self, dirname, recursive=False, allow_recreate=False):
        if not dirname and not allow_recreate:
            raise PathError(dirname)
        fullpath = normpath(dirname)
        if fullpath in ('', '/'):
            if allow_recreate:
                return
            raise DestinationExistsError(dirname)
        dirpath, dirname = pathsplit(dirname.rstrip('/'))

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


    #@synchronize
    #def _orphan_files(self, file_dir_entry):
    #    for f in file_dir_entry.open_files[:]:
    #        f.close()


    @synchronize
    @iotools.filelike_to_stream
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        path = normpath(path)
        filepath, filename = pathsplit(path)
        parent_dir_entry = self._get_dir_entry(filepath)

        if parent_dir_entry is None or not parent_dir_entry.isdir():
            raise ResourceNotFoundError(path)

        if 'r' in mode or 'a' in mode:
            if filename not in parent_dir_entry.contents:
                raise ResourceNotFoundError(path)

            file_dir_entry = parent_dir_entry.contents[filename]
            if file_dir_entry.isdir():
                raise ResourceInvalidError(path)

            file_dir_entry.accessed_time = datetime.datetime.now()

            mem_file = self.file_factory(path, self, file_dir_entry.mem_file, mode, file_dir_entry.lock)
            file_dir_entry.open_files.append(mem_file)
            return mem_file

        elif 'w' in mode:
            if filename not in parent_dir_entry.contents:
                file_dir_entry = self._make_dir_entry("file", filename)
                parent_dir_entry.contents[filename] = file_dir_entry
            else:
                file_dir_entry = parent_dir_entry.contents[filename]

            file_dir_entry.accessed_time = datetime.datetime.now()

            mem_file = self.file_factory(path, self, file_dir_entry.mem_file, mode, file_dir_entry.lock)
            file_dir_entry.open_files.append(mem_file)
            return mem_file

        if parent_dir_entry is None:
            raise ResourceNotFoundError(path)

    @synchronize
    def remove(self, path):
        dir_entry = self._get_dir_entry(path)

        if dir_entry is None:
            raise ResourceNotFoundError(path)

        if dir_entry.isdir():
            raise ResourceInvalidError(path, msg="That's a directory, not a file: %(path)s")

        pathname, dirname = pathsplit(path)
        parent_dir = self._get_dir_entry(pathname)
        del parent_dir.contents[dirname]

    @synchronize
    def removedir(self, path, recursive=False, force=False):
        path = normpath(path)
        if path in ('', '/'):
            raise RemoveRootError(path)
        dir_entry = self._get_dir_entry(path)

        if dir_entry is None:
            raise ResourceNotFoundError(path)
        if not dir_entry.isdir():
            raise ResourceInvalidError(path, msg="Can't remove resource, its not a directory: %(path)s" )

        if dir_entry.contents and not force:
            raise DirectoryNotEmptyError(path)

        if recursive:
            rpathname = path
            while rpathname:
                rpathname, dirname = pathsplit(rpathname)
                parent_dir = self._get_dir_entry(rpathname)
                if not dirname:
                    raise RemoveRootError(path)
                del parent_dir.contents[dirname]
                # stop recursing if the directory has other contents
                if parent_dir.contents:
                    break
        else:
            pathname, dirname = pathsplit(path)
            parent_dir = self._get_dir_entry(pathname)
            if not dirname:
                raise RemoveRootError(path)
            del parent_dir.contents[dirname]

    @synchronize
    def rename(self, src, dst):
        src = normpath(src)
        dst = normpath(dst)
        src_dir, src_name = pathsplit(src)
        src_entry = self._get_dir_entry(src)
        if src_entry is None:
            raise ResourceNotFoundError(src)
        open_files = src_entry.open_files[:]
        for f in open_files:
            f.flush()
            f.path = dst

        dst_dir,dst_name = pathsplit(dst)
        dst_entry = self._get_dir_entry(dst)
        if dst_entry is not None:
            raise DestinationExistsError(dst)

        src_dir_entry = self._get_dir_entry(src_dir)
        src_xattrs = src_dir_entry.xattrs.copy()
        dst_dir_entry = self._get_dir_entry(dst_dir)
        if dst_dir_entry is None:
            raise ParentDirectoryMissingError(dst)
        dst_dir_entry.contents[dst_name] = src_dir_entry.contents[src_name]
        dst_dir_entry.contents[dst_name].name = dst_name
        dst_dir_entry.xattrs.update(src_xattrs)
        del src_dir_entry.contents[src_name]

    @synchronize
    def settimes(self, path, accessed_time=None, modified_time=None):
        now = datetime.datetime.now()
        if accessed_time is None:
            accessed_time = now
        if modified_time is None:
            modified_time = now

        dir_entry = self._get_dir_entry(path)
        if dir_entry is not None:
            dir_entry.accessed_time = accessed_time
            dir_entry.modified_time = modified_time
            return True
        return False

    @synchronize
    def _on_close_memory_file(self, open_file, path):
        dir_entry = self._get_dir_entry(path)
        if dir_entry is not None and open_file in dir_entry.open_files:
            dir_entry.open_files.remove(open_file)


    @synchronize
    def _on_modify_memory_file(self, path):
        dir_entry = self._get_dir_entry(path)
        if dir_entry is not None:
            dir_entry.modified_time = datetime.datetime.now()

    @synchronize
    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        dir_entry = self._get_dir_entry(path)
        if dir_entry is None:
            raise ResourceNotFoundError(path)
        if dir_entry.isfile():
            raise ResourceInvalidError(path, msg="not a directory: %(path)s")
        paths = dir_entry.contents.keys()
        for (i,p) in enumerate(paths):
            if not isinstance(p,unicode):
                paths[i] = unicode(p)
        return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)

    @synchronize
    def getinfo(self, path):
        dir_entry = self._get_dir_entry(path)

        if dir_entry is None:
            raise ResourceNotFoundError(path)

        info = {}
        info['created_time'] = dir_entry.created_time
        info['modified_time'] = dir_entry.modified_time
        info['accessed_time'] = dir_entry.accessed_time

        if dir_entry.isdir():
            info['st_mode'] = 0755 | stat.S_IFDIR
        else:
            info['size'] = len(dir_entry.data or b(''))
            info['st_mode'] = 0666 | stat.S_IFREG

        return info

    @synchronize
    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=1024*64):
        src_dir_entry = self._get_dir_entry(src)
        if src_dir_entry is None:
            raise ResourceNotFoundError(src)
        src_xattrs = src_dir_entry.xattrs.copy()
        super(MemoryFS, self).copydir(src, dst, overwrite, ignore_errors=ignore_errors, chunk_size=chunk_size)
        dst_dir_entry = self._get_dir_entry(dst)
        if dst_dir_entry is not None:
            dst_dir_entry.xattrs.update(src_xattrs)

    @synchronize
    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=1024*64):
        src_dir_entry = self._get_dir_entry(src)
        if src_dir_entry is None:
            raise ResourceNotFoundError(src)
        src_xattrs = src_dir_entry.xattrs.copy()
        super(MemoryFS, self).movedir(src, dst, overwrite, ignore_errors=ignore_errors, chunk_size=chunk_size)
        dst_dir_entry = self._get_dir_entry(dst)
        if dst_dir_entry is not None:
            dst_dir_entry.xattrs.update(src_xattrs)

    @synchronize
    def copy(self, src, dst, overwrite=False, chunk_size=1024*64):
        src_dir_entry = self._get_dir_entry(src)
        if src_dir_entry is None:
            raise ResourceNotFoundError(src)
        src_xattrs = src_dir_entry.xattrs.copy()
        super(MemoryFS, self).copy(src, dst, overwrite, chunk_size)
        dst_dir_entry = self._get_dir_entry(dst)
        if dst_dir_entry is not None:
            dst_dir_entry.xattrs.update(src_xattrs)

    @synchronize
    def move(self, src, dst, overwrite=False, chunk_size=1024*64):
        src_dir_entry = self._get_dir_entry(src)
        if src_dir_entry is None:
            raise ResourceNotFoundError(src)
        src_xattrs = src_dir_entry.xattrs.copy()
        super(MemoryFS, self).move(src, dst, overwrite, chunk_size)
        dst_dir_entry = self._get_dir_entry(dst)
        if dst_dir_entry is not None:
            dst_dir_entry.xattrs.update(src_xattrs)

    @synchronize
    def getcontents(self, path, mode="rb", encoding=None, errors=None, newline=None):
        dir_entry = self._get_dir_entry(path)
        if dir_entry is None:
            raise ResourceNotFoundError(path)
        if not dir_entry.isfile():
            raise ResourceInvalidError(path, msg="not a file: %(path)s")
        data = dir_entry.data or b('')
        if 'b' not in mode:
            return iotools.decode_binary(data, encoding=encoding, errors=errors, newline=newline)
        return data

    @synchronize
    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=1024*64):
        if isinstance(data, six.binary_type):
            if not self.exists(path):
                self.open(path, 'wb').close()
            dir_entry = self._get_dir_entry(path)
            if not dir_entry.isfile():
                raise ResourceInvalidError('Not a directory %(path)s', path)
            new_mem_file = StringIO()
            new_mem_file.write(data)
            dir_entry.mem_file = new_mem_file
            return len(data)

        return super(MemoryFS, self).setcontents(path, data=data, encoding=encoding, errors=errors, chunk_size=chunk_size)

        # if isinstance(data, six.text_type):
        #     return super(MemoryFS, self).setcontents(path, data, encoding=encoding, errors=errors, chunk_size=chunk_size)
        # if not self.exists(path):
        #     self.open(path, 'wb').close()

        # dir_entry = self._get_dir_entry(path)
        # if not dir_entry.isfile():
        #     raise ResourceInvalidError('Not a directory %(path)s', path)
        # new_mem_file = StringIO()
        # new_mem_file.write(data)
        # dir_entry.mem_file = new_mem_file

    @synchronize
    def setxattr(self, path, key, value):
        dir_entry = self._dir_entry(path)
        key = unicode(key)
        dir_entry.xattrs[key] = value

    @synchronize
    def getxattr(self, path, key, default=None):
        key = unicode(key)
        dir_entry = self._dir_entry(path)
        return dir_entry.xattrs.get(key, default)

    @synchronize
    def delxattr(self, path, key):
        dir_entry = self._dir_entry(path)
        try:
            del dir_entry.xattrs[key]
        except KeyError:
            pass

    @synchronize
    def listxattrs(self, path):
        dir_entry = self._dir_entry(path)
        return dir_entry.xattrs.keys()
