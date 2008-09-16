#!/usr/bin/env python

from fs import *

from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED
from memoryfs import MemoryFS

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import tempfs

class _TempWriteFile(object):

    def __init__(self, fs, filename, close_callback):
        self.fs = fs
        self.filename = filename
        self._file = self.fs.open(filename, 'w+')
        self.close_callback = close_callback

    def write(self, data):
        return self._file.write(data)

    def tell(self):
        return self._file.tell()

    def close(self):
        self._file.close()
        self.close_callback(self.filename)

class _ExceptionProxy(object):

    def __getattr__(self, name):
        raise ValueError("Zip file has been closed")

    def __setattr__(self, name, value):
        raise ValueError("Zip file has been closed")

    def __nonzero__(self):
        return False

class ZipFS(FS):

    def __init__(self, zip_file, mode="r", compression="deflated", allowZip64=False, thread_syncronize=True):
        FS.__init__(self, thread_syncronize=thread_syncronize)
        if compression == "deflated":
            compression_type = ZIP_DEFLATED
        elif compression == "stored":
            compression_type = ZIP_STORED
        else:
            raise ValueError("Compression should be 'deflated' (default) or 'stored'")

        if len(mode) > 1 or mode not in "rwa":
            raise ValueError("mode must be 'r', 'w' or 'a'")

        self.zip_mode = mode
        self.zf = ZipFile(zip_file, mode, compression_type, allowZip64)
        self.zip_path = str(zip_file)

        self.temp_fs = None
        if mode in 'wa':
            self.temp_fs = tempfs.TempFS()

        self._path_fs = MemoryFS()
        if mode in 'ra':
            self._parse_resource_list()

    def __str__(self):
        return "<ZipFS: %s>" % self.zip_path

    def __unicode__(self):
        return unicode(self.__str__())

    def _parse_resource_list(self):
        for path in self.zf.namelist():
            self._add_resource(path)

    def _add_resource(self, path):
        if path.endswith('/'):
            path = path[:-1]
            self._path_fs.makedir(path, recursive=True, allow_recreate=True)
        else:
            dirpath, filename = pathsplit(path)
            if dirpath:
                self._path_fs.makedir(dirpath, recursive=True, allow_recreate=True)
            f = self._path_fs.open(path, 'w')
            f.close()


    def close(self):
        """Finalizes the zip file so that it can be read.
        No further operations will work after this method is called."""
        self._lock.acquire()
        try:
            if self.zf:
                self.zf.close()
                self.zf = _ExceptionProxy()
        finally:
            self._lock.release()

    def __del__(self):
        self.close()

    def open(self, path, mode="r", **kwargs):

        self._lock.acquire()
        try:
            path = normpath(path)
            self.zip_path = path

            if 'r' in mode:
                if self.zip_mode not in 'ra':
                    raise OperationFailedError("OPEN_FAILED", path=path, msg="Zip file must be opened for reading ('r') or appending ('a')")
                try:
                    contents = self.zf.read(path)
                except KeyError:
                    raise ResourceNotFoundError("NO_FILE", path)
                return StringIO(contents)

            if 'w' in mode:
                dirname, filename = pathsplit(path)
                if dirname:
                    self.temp_fs.makedir(dirname, recursive=True, allow_recreate=True)

                self._add_resource(path)
                f = _TempWriteFile(self.temp_fs, path, self._on_write_close)

                return f

            raise ValueError("Mode must contain be 'r' or 'w'")
        finally:
            self._lock.release()

    def getcontents(self, path):
        self._lock.acquire()
        try:
            if not exists(path):
                raise ResourceNotFoundError("NO_FILE", path)
            path = normpath(path)
            try:
                contents = self.zf.read(path)
            except KeyError:
                raise ResourceNotFoundError("NO_FILE", path)
            return contents
        finally:
            self._lock.release()

    def _on_write_close(self, filename):
        self._lock.acquire()
        try:
            sys_path = self.temp_fs.getsyspath(filename)
            self.zf.write(sys_path, filename)
        except:
            self._lock.release()

    def desc(self, path):
        if self.isdir(path):
            return "Dir in zip file: %s" % self.zip_path
        else:
            return "File in zip file: %s" % self.zip_path

    def isdir(self, path):
        return self._path_fs.isdir(path)

    def isfile(self, path):
        return self._path_fs.isdir(path)

    def exists(self, path):
        return self._path_fs.exists(path)

    def makedir(self, dirname, mode=0777, recursive=False, allow_recreate=False):
        self._lock.acquire()
        try:
            dirname = normpath(dirname)
            if self.zip_mode not in "wa":
                raise OperationFailedError("MAKEDIR_FAILED", dirname, "Zip file must be opened for writing ('w') or appending ('a')")
            if not dirname.endswith('/'):
                dirname += '/'
            self._add_resource(dirname)
        finally:
            self._lock.release()

    def listdir(self, path="/", wildcard=None, full=False, absolute=False, hidden=False, dirs_only=False, files_only=False):

        return self._path_fs.listdir(path, wildcard, full, absolute, hidden, dirs_only, files_only)


    def getinfo(self, path):
        self._lock.acquire()
        try:
            if not self.exists(path):
                return ResourceNotFoundError("NO_RESOURCE", path)
            path = normpath(path).lstrip('/')
            try:
                zi = self.zf.getinfo(path)
                zinfo = dict((attrib, getattr(zi, attrib)) for attrib in dir(zi) if not attrib.startswith('_'))
            except KeyError:
                zinfo = {'file_size':0}
            info = {'size' : zinfo['file_size'] }
            if 'date_time' in zinfo:
                info['created_time'] = datetime.datetime(*zinfo['date_time'])
            info.update(zinfo)
            return info
        finally:
            self._lock.release()

if __name__ == "__main__":
    def test():
        zfs = ZipFS("t.zip", "w")
        zfs.createfile("t.txt", "Hello, World!")
        zfs.close()
        rfs = ZipFS("t.zip", 'r')
        print rfs.getcontents("t.txt")
        print rfs.getcontents("w.txt")

    def test2():
        zfs = ZipFS("t2.zip", "r")
        print zfs.listdir("/tagging-trunk")
        print zfs.listdir("/")
        import browsewin
        browsewin.browse(zfs)
        zfs.close()
        #zfs.open("t.txt")
        #print zfs.listdir("/")

    test2()

    zfs = ZipFS("t3.zip", "w")
    zfs.createfile("t.txt", "Hello, World!")
    zfs.createfile("foo/bar/baz/t.txt", "Hello, World!")
    #print zfs.isdir("t.txt")
    #print zfs.isfile("t.txt")
    #print zfs.isfile("foo/bar")
    zfs.close()
    zfs = ZipFS("t3.zip", "r")
    print "--"
    print zfs.listdir("foo")
    print zfs.isdir("foo/bar")
    print zfs.listdir("foo/bar")
    print zfs.listdir("foo/bar/baz")
    print_fs(zfs)


    #zfs = ZipFS("t3.zip", "r")
    #print zfs.zf.getinfo("asd.txt")

    #zfs.close()
