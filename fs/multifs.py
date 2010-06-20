"""
fs.multifs
==========

A MultiFS is a filesytem composed of a sequence of other filesystems, where
the directory structure of each filesystem is overlaid over the previous filesystem.
When you attempt to access a file from the MultiFS it will try each 'child'
FS in order, until it either finds a path that exists or raises a ResourceNotFoundError.

One use for such a filesystem would be to selectively override a set of files,
to customize behaviour. For example, to create a filesystem that could be used
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

    themed_template_fs.addfs('templates', OSFS('templates'))
    themed_template_fs.addfs('theme', OSFS('themes'))
    

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


"""

from fs.base import FS, FSError, synchronize
from fs.path import *
from fs import _thread_synchronize_default
from fs.errors import ResourceNotFoundError


class MultiFS(FS):

    """A MultiFS is a filesystem that delegates to a sequence of other filesystems.
    Operations on the MultiFS will try each 'child' filesystem in order, until it
    succeeds. In effect, creating a filesystem that combines the files and dirs of
    its children.

    """

    def __init__(self):
        super(MultiFS, self).__init__(thread_synchronize=_thread_synchronize_default)

        self.fs_sequence = []
        self.fs_lookup =  {}

    @synchronize
    def __str__(self):
        return "<MultiFS: %s>" % ", ".join(str(fs) for fs in self.fs_sequence)

    __repr__ = __str__

    @synchronize
    def __unicode__(self):
        return u"<MultiFS: %s>" % ", ".join(unicode(fs) for fs in self.fs_sequence)


    @synchronize
    def addfs(self, name, fs):
        """Adds a filesystem to the MultiFS.

        :param name: A unique name to refer to the filesystem being added
        :param fs: The filesystem to add

        """
        if name in self.fs_lookup:
            raise ValueError("Name already exists.")

        self.fs_sequence.append(fs)
        self.fs_lookup[name] = fs

    @synchronize
    def removefs(self, name):
        """Removes a filesystem from the sequence.

        :param name: The name of the filesystem, as used in addfs

        """
        if name not in self.fs_lookup:
            raise ValueError("No filesystem called '%s'"%name)
        fs = self.fs_lookup[name]
        self.fs_sequence.remove(fs)
        del self.fs_lookup[name]

    @synchronize
    def __getitem__(self, name):
        return self.fs_lookup[name]

    @synchronize
    def __iter__(self):
        return reversed(self.fs_sequence[:])

    def _delegate_search(self, path):
        for fs in self:
            if fs.exists(path):
                return fs
        return None

    @synchronize
    def which(self, path):
        """Retrieves the filesystem that a given path would delegate to.
        Returns a tuple of the filesystem's name and the filesystem object itself.

        :param path: A path in MultiFS

        """
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
        raise ResourceNotFoundError(path)

    @synchronize
    def desc(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)

        name, fs = self.which(path)
        if name is None:
            return ""
        return "%s, on %s (%s)" % (fs.desc(path), name, fs)

    @synchronize
    def open(self, path, mode="r", **kwargs):
        for fs in self:
            if fs.exists(path):
                fs_file = fs.open(path, mode, **kwargs)
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
            except FSError, e:
                pass

        return list(set(paths))

    @synchronize
    def remove(self, path):
        for fs in self:
            if fs.exists(path):
                fs.remove(path)
                return
        raise ResourceNotFoundError(path)

    @synchronize
    def removedir(self, path, recursive=False):
        for fs in self:
            if fs.isdir(path):
                fs.removedir(path, recursive)
                return
        raise ResourceNotFoundError(path)

    @synchronize
    def rename(self, src, dst):
        for fs in self:
            if fs.exists(src):
                fs.rename(src, dst)
                return
        raise ResourceNotFoundError(path)

    @synchronize
    def settimes(self, path, accessed_time=None, modified_time=None):
        for fs in self:
            if fs.exists(path):
                return fs.settimes(path, accessed_time, modified_time)
        raise ResourceNotFoundError(path)

    @synchronize
    def getinfo(self, path):
        for fs in self:
            if fs.exists(path):
                return fs.getinfo(path)
        raise ResourceNotFoundError(path)
