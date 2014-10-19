"""
fs.multifs
==========

A MultiFS is a filesystem composed of a sequence of other filesystems, where
the directory structure of each filesystem is overlaid over the previous
filesystem. When you attempt to access a file from the MultiFS it will try
each 'child' FS in order, until it either finds a path that exists or raises a
ResourceNotFoundError.

One use for such a filesystem would be to selectively override a set of files,
to customize behavior. For example, to create a filesystem that could be used
to *theme* a web application. We start with the following directories::


    `-- templates
        |-- snippets
        |   `-- panel.html
        |-- index.html
        |-- profile.html
        `-- base.html

    `-- theme
        |-- snippets
        |   |-- widget.html
        |   `-- extra.html
        |-- index.html
        `-- theme.html

And we want to create a single filesystem that looks for files in `templates` if
they don't exist in `theme`. We can do this with the following code::


    from fs.osfs import OSFS
    from fs.multifs import MultiFS

    themed_template_fs = MultiFS()
    themed_template_fs.addfs('templates', OSFS('templates'))
    themed_template_fs.addfs('theme', OSFS('theme'))


Now we have a `themed_template_fs` FS object presents a single view of both
directories::

        |-- snippets
        |   |-- panel.html
        |   |-- widget.html
        |   `-- extra.html
        |-- index.html
        |-- profile.html
        |-- base.html
        `-- theme.html

A MultiFS is generally read-only, and any operation that may modify data
(including opening files for writing) will fail. However, you can set a
writeable fs with the `setwritefs` method -- which does not have to be
one of the FS objects set with `addfs`.

The reason that only one FS object is ever considered for write access is
that otherwise it would be ambiguous as to which filesystem you would want
to modify. If you need to be able to modify more than one FS in the MultiFS,
you can always access them directly.

"""

from fs.base import FS, synchronize
from fs.path import *
from fs.errors import *
from fs import _thread_synchronize_default


class MultiFS(FS):

    """A filesystem that delegates to a sequence of other filesystems.

    Operations on the MultiFS will try each 'child' filesystem in order, until
    it succeeds. In effect, creating a filesystem that combines the files and
    dirs of its children.
    """

    _meta = { 'virtual': True,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False
              }

    def __init__(self, auto_close=True):
        """

        :param auto_close: If True the child filesystems will be closed when the MultiFS is closed

        """
        super(MultiFS, self).__init__(thread_synchronize=_thread_synchronize_default)

        self.auto_close = auto_close
        self.fs_sequence = []
        self.fs_lookup =  {}
        self.fs_priorities = {}
        self.writefs = None

    @synchronize
    def __str__(self):
        return "<MultiFS: %s>" % ", ".join(str(fs) for fs in self.fs_sequence)

    __repr__ = __str__

    @synchronize
    def __unicode__(self):
        return u"<MultiFS: %s>" % ", ".join(unicode(fs) for fs in self.fs_sequence)

    def _get_priority(self, name):
        return self.fs_priorities[name]

    @synchronize
    def close(self):
        # Explicitly close if requested
        if self.auto_close:
            for fs in self.fs_sequence:
                fs.close()
            if self.writefs is not None:
                self.writefs.close()
        # Discard any references
        del self.fs_sequence[:]
        self.fs_lookup.clear()
        self.fs_priorities.clear()
        self.writefs = None
        super(MultiFS, self).close()

    def _priority_sort(self):
        """Sort filesystems by priority order"""
        priority_order = sorted(self.fs_lookup.keys(), key=lambda n: self.fs_priorities[n], reverse=True)
        self.fs_sequence = [self.fs_lookup[name] for name in priority_order]

    @synchronize
    def addfs(self, name, fs, write=False, priority=0):
        """Adds a filesystem to the MultiFS.

        :param name: A unique name to refer to the filesystem being added.
            The filesystem can later be retrieved by using this name as an index to the MultiFS, i.e. multifs['myfs']
        :param fs: The filesystem to add
        :param write: If this value is True, then the `fs` will be used as the writeable FS
        :param priority: A number that gives the priorty of the filesystem being added.
            Filesystems will be searched in descending priority order and then by the reverse order they were added.
            So by default, the most recently added filesystem will be looked at first


        """
        if name in self.fs_lookup:
            raise ValueError("Name already exists.")

        priority = (priority, len(self.fs_sequence))
        self.fs_priorities[name] = priority
        self.fs_sequence.append(fs)
        self.fs_lookup[name] = fs

        self._priority_sort()

        if write:
            self.setwritefs(fs)

    @synchronize
    def setwritefs(self, fs):
        """Sets the filesystem to use when write access is required. Without a writeable FS,
        any operations that could modify data (including opening files for writing / appending)
        will fail.

        :param fs: An FS object that will be used to open writeable files

        """
        self.writefs = fs

    @synchronize
    def clearwritefs(self):
        """Clears the writeable filesystem (operations that modify the multifs will fail)"""
        self.writefs = None

    @synchronize
    def removefs(self, name):
        """Removes a filesystem from the sequence.

        :param name: The name of the filesystem, as used in addfs

        """
        if name not in self.fs_lookup:
            raise ValueError("No filesystem called '%s'" % name)
        fs = self.fs_lookup[name]
        self.fs_sequence.remove(fs)
        del self.fs_lookup[name]
        self._priority_sort()

    @synchronize
    def __getitem__(self, name):
        return self.fs_lookup[name]

    @synchronize
    def __iter__(self):
        return iter(self.fs_sequence[:])

    def _delegate_search(self, path):
        for fs in self:
            if fs.exists(path):
                return fs
        return None

    @synchronize
    def which(self, path, mode='r'):
        """Retrieves the filesystem that a given path would delegate to.
        Returns a tuple of the filesystem's name and the filesystem object itself.

        :param path: A path in MultiFS

        """
        if 'w' in mode or '+' in mode or 'a' in mode:
            return self.writefs
        for fs in self:
            if fs.exists(path):
                for fs_name, fs_object in self.fs_lookup.iteritems():
                    if fs is fs_object:
                        return fs_name, fs
        raise ResourceNotFoundError(path, msg="Path does not map to any filesystem: %(path)s")

    @synchronize
    def getsyspath(self, path, allow_none=False):
        fs = self._delegate_search(path)
        if fs is not None:
            return fs.getsyspath(path, allow_none=allow_none)
        if allow_none:
            return None
        raise ResourceNotFoundError(path)

    @synchronize
    def desc(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)

        name, fs = self.which(path)
        if name is None:
            return ""
        return "%s (in %s)" % (fs.desc(path), name)

    @synchronize
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        if 'w' in mode or '+' in mode or 'a' in mode:
            if self.writefs is None:
                raise OperationFailedError('open', path=path, msg="No writeable FS set")
            return self.writefs.open(path, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, line_buffering=line_buffering, **kwargs)
        for fs in self:
            if fs.exists(path):
                fs_file = fs.open(path, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, line_buffering=line_buffering, **kwargs)
                return fs_file
        raise ResourceNotFoundError(path)

    @synchronize
    def exists(self, path):
        return self._delegate_search(path) is not None

    @synchronize
    def isdir(self, path):
        fs = self._delegate_search(path)
        if fs is not None:
            return fs.isdir(path)
        return False

    @synchronize
    def isfile(self, path):
        fs = self._delegate_search(path)
        if fs is not None:
            return fs.isfile(path)
        return False

    @synchronize
    def listdir(self, path="./", *args, **kwargs):
        paths = []
        for fs in self:
            try:
                paths += fs.listdir(path, *args, **kwargs)
            except FSError:
                pass
        return list(set(paths))

    @synchronize
    def makedir(self, path, recursive=False, allow_recreate=False):
        if self.writefs is None:
            raise OperationFailedError('makedir', path=path, msg="No writeable FS set")
        self.writefs.makedir(path, recursive=recursive, allow_recreate=allow_recreate)

    @synchronize
    def remove(self, path):
        if self.writefs is None:
            raise OperationFailedError('remove', path=path, msg="No writeable FS set")
        self.writefs.remove(path)

    @synchronize
    def removedir(self, path, recursive=False, force=False):
        if self.writefs is None:
            raise OperationFailedError('removedir', path=path, msg="No writeable FS set")
        if normpath(path) in ('', '/'):
            raise RemoveRootError(path)
        self.writefs.removedir(path, recursive=recursive, force=force)

    @synchronize
    def rename(self, src, dst):
        if self.writefs is None:
            raise OperationFailedError('rename', path=src, msg="No writeable FS set")
        self.writefs.rename(src, dst)

    @synchronize
    def settimes(self, path, accessed_time=None, modified_time=None):
        if self.writefs is None:
            raise OperationFailedError('settimes', path=path, msg="No writeable FS set")
        self.writefs.settimes(path, accessed_time, modified_time)

    @synchronize
    def getinfo(self, path):
        for fs in self:
            if fs.exists(path):
                return fs.getinfo(path)
        raise ResourceNotFoundError(path)
