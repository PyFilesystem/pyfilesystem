"""
fs.wrapfs.subfs
===============

An FS wrapper class for accessing just a subdirectory for an FS.

"""

from fs.wrapfs import WrapFS
from fs.errors import *
from fs.path import *


class SubFS(WrapFS):
    """A SubFS represents a sub directory of another filesystem object.

    SubFS objects are returned by opendir, which effectively creates a
    'sandbox' filesystem that can only access files/dirs under a root path
    within its 'parent' dir.
    """

    def __init__(self, wrapped_fs, sub_dir):
        self.sub_dir = abspath(normpath(sub_dir))
        super(SubFS, self).__init__(wrapped_fs)

    def _encode(self, path):
        return pathjoin(self.sub_dir, relpath(normpath(path)))

    def _decode(self, path):
        return abspath(normpath(path))[len(self.sub_dir):]

    def __str__(self):
        #return self.wrapped_fs.desc(self.sub_dir)
        return '<SubFS: %s/%s>' % (self.wrapped_fs, self.sub_dir.lstrip('/'))

    def __unicode__(self):
        return u'<SubFS: %s/%s>' % (self.wrapped_fs, self.sub_dir.lstrip('/'))

    def __repr__(self):
        return "SubFS(%r, %r)" % (self.wrapped_fs, self.sub_dir)

    def desc(self, path):
        if path in ('', '/'):
            return self.wrapped_fs.desc(self.sub_dir)
        return '%s!%s' % (self.wrapped_fs.desc(self.sub_dir), path)

    def setcontents(self, path, data, encoding=None, errors=None, chunk_size=64*1024):
        path = self._encode(path)
        return self.wrapped_fs.setcontents(path, data, chunk_size=chunk_size)

    def opendir(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        path = self._encode(path)
        return self.wrapped_fs.opendir(path)

    def close(self):
        self.closed = True

    def removedir(self, path, recursive=False, force=False):
        # Careful not to recurse outside the subdir
        path = normpath(path)
        if path in ('', '/'):
            raise RemoveRootError(path)
        super(SubFS, self).removedir(path, force=force)
        if recursive:
            try:
                if dirname(path) not in ('', '/'):
                    self.removedir(dirname(path), recursive=True)
            except DirectoryNotEmptyError:
                pass

#        if path in ("","/"):
#            if not force:
#                for path2 in self.listdir(path):
#                    raise DirectoryNotEmptyError(path)
#            else:
#                for path2 in self.listdir(path,absolute=True,files_only=True):
#                    try:
#                        self.remove(path2)
#                    except ResourceNotFoundError:
#                        pass
#                for path2 in self.listdir(path,absolute=True,dirs_only=True):
#                    try:
#                        self.removedir(path2,force=True)
#                    except ResourceNotFoundError:
#                        pass
#        else:
#            super(SubFS,self).removedir(path,force=force)
#            if recursive:
#                try:
#                    if dirname(path):
#                        self.removedir(dirname(path),recursive=True)
#                except DirectoryNotEmptyError:
#                    pass


