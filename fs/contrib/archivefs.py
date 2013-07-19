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
        if 'r' in mode:
            for item in self.archive:
                for part in recursepath(item.pathname)[1:]:
                    part = relpath(part)
                    if part == item.pathname:
                        self.contents[part] = item
                    else:
                        self.contents[part] = libarchive.Entry(pathname=part, mode=stat.S_IFDIR, size=0, mtime=item.mtime)

    def __str__(self):
        return "<ArchiveFS: %s>" % self.root_path

    def __unicode__(self):
        return u"<ArchiveFS: %s>" % self.root_path

    def getmeta(self, meta_name, default=NoDefaultMeta):
        if meta_name == 'read_only':
            return self.read_only
        return super(ArchiveFS, self).getmeta(meta_name, default)

    @synchronize
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
    def getcontents(self, path, mode="rb", encoding=None, errors=None, newline=None):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        with self.open(path, mode, encoding=encoding, errors=errors, newline=newline) as f:
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
                value = getattr(entry, attr)
                if callable(value):
                    continue
                info[attr] = value
        return info

    def getsize(self, path):
        return self.getinfo(path)['st_size']


class ArchiveMountFS(mountfs.MountFS):
    '''A subclass of MountFS that automatically identifies archives. Once identified
    archives are mounted in place of the archive file.'''

    def __init__(self, rootfs, auto_close=True, auto_mount=True, max_size=None):
        self.auto_mount = auto_mount
        self.max_size = max_size
        super(ArchiveMountFS, self).__init__(auto_close=auto_close)
        self.rootfs = rootfs
        self.mountdir('/', rootfs)

    @synchronize
    def close(self):
        # Close and delete references to any other fs instances.
        if self.rootfs is not None:
            self.rootfs.close()
            self.rootfs = None
        super(ArchiveMountFS, self).close()

    def ismount(self, path):
        "Checks if the given path has a file system mounted on it."
        try:
            object = self.mount_tree[path]
        except KeyError:
            return False
        return isinstance(object, mountfs.MountFS.DirMount)

    def unmount(self, path):
        """Unmounts a path.

        :param path: Path to unmount

        """
        # This might raise a KeyError, but that is what MountFS will do, so
        # shall we.
        fs = self.mount_tree.pop(path)
        # TODO: it may be necessary to remember what paths were auto-mounted,
        # so we can close those here. It may not be safe to close a file system
        # that the user provided. However, it is definitely NOT safe to leave
        # one open.
        if callable(getattr(fs, 'close', None)):
            fs.close()

    def _delegate(self, path, auto_mount=True):
        """A _delegate() override that will automatically mount archives that are
        encountered in the path. For example, the path /foo/bar.zip/baz.txt contains
        the archive path /foo/bar.zip. If this archive can be mounted by ArchiveFS,
        it will be. Then the file system call will be delegated to that mounted file
        system, which will act upon /baz.txt within the archive. This is lazy
        initialization which means users of this class need not crawl the file system
        for archives and mount them all up-front.

        This behavior can be overridden by self.auto_mount=False or by passing the
        auto_mount=False keyword argument.
        """
        if self.auto_mount and auto_mount:
            for ppath in recursepath(path)[1:]:
                if self.ismount(ppath):
                    # If something is already mounted here, no need to continue.
                    break
                if libarchive.is_archive_name(ppath):
                    # It looks like an archive, we might mount it.
                    # First check that the size is acceptable.
                    if self.max_size:
                        if self.rootfs.exists(ppath) and \
                           self.rootfs.getsize(ppath) > self.max_size:
                            break
                    # Looks good, the proof is in the pudding, so let's try to
                    # mount this *supposed* archive...
                    full_path = self.rootfs.getsyspath(ppath)
                    try:
                        # TODO: it would be really nice if we could open the path using
                        # self.rootfs.open(), that way we could support archives on a file
                        # system other than osfs (even nested archives). However, the libarchive
                        # wrapper is not sophisticated enough to handle a Python file-like object,
                        # it uses an actual fd.
                        self.mountdir(ppath, ArchiveFS(full_path, 'r'))
                        # That worked!! Stop recursing path, we support just one archive per path!
                        break
                    except:
                        # Must NOT have been an archive after all, but maybe
                        # there is one deeper in the directory...
                        continue
        return super(ArchiveMountFS, self)._delegate(path)

    def getsyspath(self, path, allow_none=False):
        """A getsyspath() override that returns paths relative to the root fs."""
        root = self.rootfs.getsyspath('/', allow_none=allow_none)
        if root:
            return join(root, path.lstrip('/'))

    def open(self, path, *args, **kwargs):
        """An open() override that opens an archive. It is not fooled by mounted
        archives. If the path is a mounted archive, it is unmounted and the archive
        file is opened and returned."""
        if libarchive.is_archive_name(path) and self.ismount(path):
            self.unmount(path)
        fs, _mount_path, delegate_path = self._delegate(path, auto_mount=False)
        return fs.open(delegate_path, *args, **kwargs)

    def getinfo(self, path):
        """A getinfo() override that allows archives to masqueraded as directories.
        If the path is not an archive, the call is delegated. In the event that the
        path is an archive, that archive is mounted to ensure it can actually be
        treaded like a directory."""
        fs, _mount_path, delegate_path = self._delegate(path)
        if isinstance(fs, ArchiveFS) and path == _mount_path:
            info = self.rootfs.getinfo(path)
            info['st_mode'] = info.get('st_mode', 0) | stat.S_IFDIR
            return info
        return super(ArchiveMountFS, self).getinfo(path)

    def isdir(self, path):
        """An isdir() override that allows archives to masquerade as directories. If
        the path is not an archive, the call is delegated. In the event that the path
        is an archive, that archive is mounted to ensure it can actually be treated
        like a directory."""
        fs, _mount_path, delegate_path = self._delegate(path)
        if isinstance(fs, ArchiveFS) and path == _mount_path:
            # If the path is an archive mount point, it is a directory.
            return True
        return super(ArchiveMountFS, self).isdir(path)

    def isfile(self, path):
        """An isfile() override that checks if the given path is a file or not. It is
        not fooled by a mounted archive. If the path is not an archive, True is returned.
        If the path is not an archive, the call is delegated."""
        fs, _mount_path, delegate_path = self._delegate(path)
        if isinstance(fs, ArchiveFS) and path == _mount_path:
            # If the path is an archive mount point, it is a file.
            return True
        else:
            return fs.isfile(delegate_path)

    def getsize(self, path):
        """A getsize() override that returns the size of an archive. It is not fooled by
        a mounted archive. If the path is not an archive, the call is delegated."""
        fs, _mount_path, delegate_path = self._delegate(path, auto_mount=False)
        if isinstance(fs, ArchiveFS) and path == _mount_path:
            return self.rootfs.getsize(path)
        else:
            return fs.getsize(delegate_path)

    def remove(self, path):
        """A remove() override that deletes an archive directly. It is not fooled
        by a mounted archive. If the path is not an archive, the call is delegated."""
        if libarchive.is_archive_name(path) and self.ismount(path):
            self.unmount(path)
        fs, _mount_path, delegate_path = self._delegate(path, auto_mount=False)
        return fs.remove(delegate_path)

    def makedir(self, path, *args, **kwargs):
        """A makedir() override that handles creation of a directory at an archive
        location properly. If the path is not an archive, the call is delegated."""
        # If the caller is trying to create a directory where an archive lives
        # we should raise an error. In the case when allow_recreate=True, this
        # call would succeed without the check below.
        fs, _mount_path, delegate_path = self._delegate(path, auto_mount=False)
        if isinstance(fs, ArchiveFS) and path == _mount_path:
            raise ResourceInvalidError(path, msg="Cannot create directory, there's "
                                       "already a file of that name: %(path)s")
        return fs.makedir(delegate_path, *args, **kwargs)

    def copy(self, src, dst, overwrite=False, chunk_size=1024*64):
        """An optimized copy() that will skip mounting an archive if one is involved
        as either the src or dst. This allows the file containing the archive to be
        copied."""
        src_is_archive = libarchive.is_archive_name(src)
        # If src path is a mounted archive, unmount it.
        if src_is_archive and self.ismount(src):
            self.unmount(src)
        # Now delegate the path, if the path is an archive, don't remount it.
        srcfs, _ignored, src = self._delegate(src, auto_mount=(not src_is_archive))
        # Follow the same steps for dst.
        dst_is_archive = libarchive.is_archive_name(dst)
        if dst_is_archive and self.ismount(dst):
            self.unmount(dst)
        dstfs, _ignored, dst = self._delegate(dst, auto_mount=(not dst_is_archive))
        # srcfs, src and dstfs, dst are now the file system and path for our src and dst.
        if srcfs is dstfs and srcfs is not self:
            # Both src and dst are on the same fs, let it do the copy.
            srcfs.copy(src, dst, overwrite=overwrite, chunk_size=chunk_size)
        else:
            # Src and dst are on different file systems. Just do the copy...
            srcfd = None
            try:
                srcfd = srcfs.open(src, 'rb')
                dstfs.setcontents(dst, srcfd, chunk_size=chunk_size)
            except ResourceNotFoundError:
                if srcfs.exists(src) and not dstfs.exists(dirname(dst)):
                    raise ParentDirectoryMissingError(dst)
            finally:
                if srcfd:
                    srcfd.close()

    def move(self, src, dst, overwrite=False, chunk_size=1024*64):
        """An optimized move() that delegates the work to the overridden copy() and
        remove() methods."""
        self.copy(src, dst, overwrite=overwrite, chunk_size=chunk_size)
        self.remove(src)

    def rename(self, src, dst):
        """An rename() implementation that ensures the rename does not span
        file systems. It also ensures that an archive can be renamed (without
        trying to mount either the src or destination paths)."""
        src_is_archive = libarchive.is_archive_name(src)
        # If src path is a mounted archive, unmount it.
        if src_is_archive and self.ismount(src):
            self.unmount(src)
        # Now delegate the path, if the path is an archive, don't remount it.
        srcfs, _ignored, src = self._delegate(src, auto_mount=(not src_is_archive))
        # Follow the same steps for dst.
        dst_is_archive = libarchive.is_archive_name(dst)
        if dst_is_archive and self.ismount(dst):
            self.unmount(dst)
        dstfs, _ignored, dst = self._delegate(dst, auto_mount=(not dst_is_archive))
        # srcfs, src and dstfs, dst are now the file system and path for our src and dst.
        if srcfs is dstfs and srcfs is not self:
            # Both src and dst are on the same fs, let it do the copy.
            return srcfs.rename(src, dst)
        raise OperationFailedError("rename resource", path=src)

    def walk(self,
             path="/",
             wildcard=None,
             dir_wildcard=None,
             search="breadth",
             ignore_errors=False,
             archives_as_files=True):
        """Walks a directory tree and yields the root path and contents.
        Yields a tuple of the path of each directory and a list of its file
        contents.

        :param path: root path to start walking
        :type path: string
        :param wildcard: if given, only return files that match this wildcard
        :type wildcard: a string containing a wildcard (e.g. `*.txt`) or a callable that takes the file path and returns a boolean
        :param dir_wildcard: if given, only walk directories that match the wildcard
        :type dir_wildcard: a string containing a wildcard (e.g. `*.txt`) or a callable that takes the directory name and returns a boolean
        :param search: a string identifying the method used to walk the directories. There are two such methods:

             * ``"breadth"`` yields paths in the top directories first
             * ``"depth"`` yields the deepest paths first

        :param ignore_errors: ignore any errors reading the directory
        :type ignore_errors: bool
        :param archives_as_files: treats archives as files rather than directories.
        :type ignore_errors: bool

        :rtype: iterator of (current_path, paths)

        """
        path = normpath(path)

        def isdir(path):
            if not self.isfile(path):
                return True
            if not archives_as_files and self.ismount(path):
                return True
            return False

        def listdir(path, *args, **kwargs):
            dirs_only = kwargs.pop('dirs_only', False)
            if ignore_errors:
                try:
                    listing = self.listdir(path, *args, **kwargs)
                except:
                    return []
            else:
                listing = self.listdir(path, *args, **kwargs)
            if dirs_only:
                listing = filter(isdir, listing)
            return listing

        if wildcard is None:
            wildcard = lambda f:True
        elif not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn:bool (wildcard_re.match(fn))

        if dir_wildcard is None:
            dir_wildcard = lambda f:True
        elif not callable(dir_wildcard):
            dir_wildcard_re = re.compile(fnmatch.translate(dir_wildcard))
            dir_wildcard = lambda fn:bool (dir_wildcard_re.match(fn))

        if search == "breadth":

            dirs = [path]
            dirs_append = dirs.append
            dirs_pop = dirs.pop
            while dirs:
                current_path = dirs_pop()
                paths = []
                paths_append = paths.append
                try:
                    for filename in listdir(current_path):
                        path = pathcombine(current_path, filename)
                        if isdir(path):
                            if dir_wildcard(path):
                                dirs_append(path)
                        else:
                            if wildcard(filename):
                                paths_append(filename)
                except ResourceNotFoundError:
                    # Could happen if another thread / process deletes something whilst we are walking
                    pass

                yield (current_path, paths)

        elif search == "depth":

            def recurse(recurse_path):
                try:
                    for path in listdir(recurse_path, wildcard=dir_wildcard, full=True, dirs_only=True):
                        for p in recurse(path):
                            yield p
                except ResourceNotFoundError:
                    # Could happen if another thread / process deletes something whilst we are walking
                    pass
                yield (recurse_path, listdir(recurse_path, wildcard=wildcard, files_only=True))

            for p in recurse(path):
                yield p

        else:
            raise ValueError("Search should be 'breadth' or 'depth'")


def main():
    ArchiveFS()


if __name__ == '__main__':
    main()

