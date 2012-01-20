"""
fs.contrib.archivefs
========

A FS object that represents the contents of an archive.

"""

import time
import stat
import datetime
import os.path

from fs.base import *
from fs.path import *
from fs.errors import *
from fs.filelike import StringIO

import libarchive

ENCODING = libarchive.ENCODING

class ArchiveFS(FS):
    """A FileSystem that represents an archive supported by libarchive."""

    _meta = { 'thread_safe' : True,
              'virtual' : False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'network' : False,
              'atomic.setcontents' : False
             }

    def __init__(self, f, mode='r', format=None, thread_synchronize=True):
        """Create a FS that maps on to a zip file.

        :param path: a (system) path, or a file-like object
        :param thread_synchronize: set to True (default) to enable thread-safety
        """
        super(ArchiveFS, self).__init__(thread_synchronize=thread_synchronize)
        self.contents = PathMap()
        self.archive = libarchive.SeekableArchive(f, format=format, mode=mode)
        if mode == 'r':
            for item in self.archive:
                for part in recursepath(item.pathname)[1:]:
                    part = relpath(part)
                    if part == item.pathname:
                        self.contents[part] = item
                    else:
                        self.contents[part] = libarchive.Entry(pathname=part, mode=stat.S_IFDIR, size=0, mtime=item.mtime)

    def __str__(self):
        return "<ArchiveFS>"

    def __unicode__(self):
        return u"<ArchiveFS>"

    def getmeta(self, meta_name, default=NoDefaultMeta):
        if meta_name == 'read_only':
            return self.read_only
        return super(ZipFS, self).getmeta(meta_name, default)

    def close(self):
        self.archive.close()

    @synchronize
    def open(self, path, mode="r", **kwargs):
        path = normpath(relpath(path))
        if mode not in ('r', 'w', 'wb'):
            raise Exception('Unsupported mode ' + mode)
        if 'r' in mode:
            return self.archive.readstream(path)
        else:
            return self.archive.writestream(path)

    @synchronize
    def getcontents(self, path, mode="rb"):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        f = self.open(path)
        return f.read()

    def desc(self, path):        
        return "%s in zip file" % path

    def isdir(self, path):
        info = self.getinfo(path)
        return stat.S_ISDIR(info.get('mode', 0))

    def isfile(self, path):
        info = self.getinfo(path)
        return stat.S_ISREG(info.get('mode', 0))

    def exists(self, path):
        path = normpath(path).lstrip('/')
        return path in self.contents

    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        return self._listdir_helper(path, self.contents.names(path), wildcard, full, absolute, dirs_only, files_only)

    def makedir(self, dirname, recursive=False, allow_recreate=False):
        entry = self.archive.entry_class(pathname=dirname, mode=stat.S_IFDIR, size=0, mtime=time.time())
        self.archive.write(entry)

    @synchronize
    def getinfo(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        path = normpath(path).lstrip('/')
        info = { 'size': 0 }
        try:
            entry = self.contents.get(path)
            for attr in dir(entry):
                if attr.startswith('_'):
                    continue
                elif attr == 'mtime':
                    info['created_time'] = datetime.datetime.fromtimestamp(entry.mtime)
                else:
                    info[attr] = getattr(entry, attr)
        except KeyError:
            pass
        return info

def main():
    ArchiveFS()

if __name__ == '__main__':
    main()

