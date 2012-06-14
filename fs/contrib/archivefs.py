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
from fs import mountfs

import libarchive

ENCODING = libarchive.ENCODING


class SizeUpdater(object):
    '''A file-like object to allow writing to a file within the archive. When closed
    this object will update the archive entry's size within the archive.'''
    def __init__(self, entry, stream):
        self.entry = entry
        self.stream = stream
        self.size = 0

    def __del__(self):
        self.close()

    def write(self, data):
        self.size += len(data)
        self.stream.write(data)

    def close(self):
        self.stream.close()
        self.entry.size = self.size


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
        """Create a FS that maps on to an archive file.

        :param f: a (system) path, or a file-like object
        :param format: required for 'w' mode. The archive format ('zip, 'tar', etc)
        :param thread_synchronize: set to True (default) to enable thread-safety
        """
        super(ArchiveFS, self).__init__(thread_synchronize=thread_synchronize)
        if isinstance(f, basestring):
            self.fileobj = None
            self.root_path = f
        else:
            self.fileobj = f
            self.root_path = getattr(f, 'name', None)
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

    def __del__(self):
        self.close()

    def __str__(self):
        return "<ArchiveFS: %s>" % self.root_path

    def __unicode__(self):
        return u"<ArchiveFS: %s>" % self.root_path

    def getmeta(self, meta_name, default=NoDefaultMeta):
        if meta_name == 'read_only':
            return self.read_only
        return super(ZipFS, self).getmeta(meta_name, default)

    def close(self):
        if getattr(self, 'archive', None) is None:
            return
        self.archive.close()

    @synchronize
    def open(self, path, mode="r", **kwargs):
        path = normpath(relpath(path))
        if path == '':
            # We need to open the archive itself, not one of it's entries.
            return file(self.root_path, mode)
        if 'a' in mode:
            raise Exception('Unsupported mode ' + mode)
        if 'r' in mode:
            return self.archive.readstream(path)
        else:
            entry = self.archive.entry_class(pathname=path, mode=stat.S_IFREG, size=0, mtime=time.time())
            self.contents[path] = entry
            return SizeUpdater(entry, self.archive.writestream(path))

    @synchronize
    def getcontents(self, path, mode="rb"):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        f = self.open(path)
        return f.read()

    def desc(self, path):
        return "%s in zip file" % path

    def getsyspath(self, path, allow_none=False):
        path = normpath(path).lstrip('/')
        return join(self.root_path, path)

    def isdir(self, path):
        info = self.getinfo(path)
        # Don't use stat.S_ISDIR, it won't work when mode == S_IFREG | S_IFDIR.
        return info.get('st_mode', 0) & stat.S_IFDIR == stat.S_IFDIR

    def isfile(self, path):
        info = self.getinfo(path)
        # Don't use stat.S_ISREG, it won't work when mode == S_IFREG | S_IFDIR.
        return info.get('st_mode', 0) & stat.S_IFREG == stat.S_IFREG

    def exists(self, path):
        path = normpath(path).lstrip('/')
        if path == '':
            # We are being asked about root (the archive itself)
            return True
        return path in self.contents

    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        return self._listdir_helper(path, self.contents.names(path), wildcard, full, absolute, dirs_only, files_only)

    def makedir(self, dirname, recursive=False, allow_recreate=False):
        entry = self.archive.entry_class(pathname=dirname, mode=stat.S_IFDIR, size=0, mtime=time.time())
        self.contents[dirname] = entry
        self.archive.write(entry)

    @synchronize
    def getinfo(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        path = normpath(path).lstrip('/')
        if path == '':
            # We are being asked about root (the archive itself)
            if self.root_path:
                st = os.stat(self.root_path)
            elif hasattr(self.fileobj, 'fileno'):
                st = os.fstat(self.fileobj.fileno())
            else:
                raise Exception('Could not stat archive.')
            info = dict((k, getattr(st, k)) for k in dir(st) if k.startswith('st_'))
            for name, longname in (
                    ('st_ctime', 'created_time'), ('st_atime', 'accessed_time'),
                    ('st_mtime', 'modified_time'),
                ):
                if name in info:
                    t = info.pop(name)
                    if t:
                        info[longname] = datetime.datetime.fromtimestamp(t)
            info['size'] = info.pop('st_size')
            # Masquerade as a directory.
            info['st_mode'] = info.get('st_mode', 0) | stat.S_IFDIR
        else:
            info = { 'size': 0 }
            entry = self.contents.get(path)
            for attr in dir(entry):
                if attr.startswith('_'):
                    continue
                elif attr == 'mtime':
                    info['modified_time'] = datetime.datetime.fromtimestamp(entry.mtime)
                elif attr == 'mode':
                    info['st_mode'] = entry.mode
                else:
                    info[attr] = getattr(entry, attr)
        return info


class ArchiveMountFS(mountfs.MountFS):
    '''A subclass of MountFS that automatically identifies archives. Once identified
    archives are mounted in place of the archive file.'''
    def __init__(self, rootfs, auto_mount=True):
        self.auto_mount = auto_mount
        super(ArchiveMountFS, self).__init__(auto_close=True)
        self.rootfs = rootfs
        self.mountdir('/', rootfs)

    def __del__(self):
        # Close automatically.
        self.close()

    def ismount(self, path):
        try:
            object = self.mount_tree[path]
        except KeyError:
            return False
        return type(object) is mountfs.MountFS.DirMount

    def _delegate(self, path):
        if self.auto_mount:
            for ppath in recursepath(path)[1:]:
                # Don't mount again...
                if self.ismount(ppath):
                    break
                if libarchive.is_archive_name(ppath):
                    # It looks like an archive, try mounting it.
                    full_path = self.rootfs.getsyspath(ppath)
                    try:
                        self.mountdir(ppath, ArchiveFS(full_path, 'r'))
                    except:
                        pass # Must NOT have been an archive after all
                    # Stop recursing path, we support just one archive per path!
                    # No nested archives yet!
                    break
        return super(ArchiveMountFS, self)._delegate(path)

    # TODO: probably need to override move(), movedir() and any other methods
    # that modify files (and therefore archives). See remove() below to see
    # why.

    def getsyspath(self, path):
        # Optimized getsyspath() that avoids calling _delegate() and thus
        # mounting an archive.
        return self.rootfs.getsyspath(path)

    def remove(self, path):
        # In case one of our mounted file systems backing archive is being
        # deleted, unmout it before continuing. Once unmounted, the archive
        # can be deleted by root fs, otherwise, the ArchiveFS will be asked
        # to remove itself, which it cannot do.
        if self.ismount(path) and libarchive.is_archive_name(path):
            self.unmount(path)
            # Send the delete directoy to the root filesystem. This avoids
            # being delegated, and the fs we just unmounted being remounted.
            return self.rootfs.remove(path)
        # Otherwise, just delegate to the responsible fs.
        return super(ArchiveMountFS, self).remove(path)

    def makedir(self, path, *args, **kwargs):
        # If the caller is trying to create a directory where an archive lives
        # we should raise an error. In the case when allow_recreate=True, this
        # call would succeed without the check below.
        if self.rootfs.isfile(path):
            raise ResourceInvalidError(path,msg="Cannot create directory, there's already a file of that name: %(path)s")
        return super(ArchiveFS, self).makedir(path, *args, **kwargs)


def main():
    ArchiveFS()

if __name__ == '__main__':
    main()

