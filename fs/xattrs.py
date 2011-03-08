"""
fs.xattrs
=========

Extended attribute support for FS

This module defines a standard interface for FS subclasses that want to
support extended file attributes, and a WrapFS subclass that can simulate
extended attributes on top of an ordinary FS.

FS instances offering extended attribute support must provide the following
methods:

  * ``getxattr(path,name)`` Get the named attribute for the given path, or None if it does not exist
  * ``setxattr(path,name,value)`` Set the named attribute for the given path to the given value
  * ``delxattr(path,name)`` Delete the named attribute for the given path, raising KeyError if it does not exist
  * ``listxattrs(path)`` Iterate over all stored attribute names for the given path

If extended attributes are required by FS-consuming code, it should use the
function 'ensure_xattrs'. This will interrogate an FS object to determine
if it has native xattr support, and return a wrapped version if it does not.
"""

import sys
try:
    import cPickle as pickle
except ImportError:
    import pickle

from fs.path import *
from fs.errors import *
from fs.wrapfs import WrapFS
from fs.base import synchronize


def ensure_xattrs(fs):
    """Ensure that the given FS supports xattrs, simulating them if required.

    Given an FS object, this function returns an equivalent FS that has support
    for extended attributes.  This may be the original object if they are
    supported natively, or a wrapper class is they must be simulated.
    
    :param fs: An FS object that must have xattrs
    """
    try:
        #  This attr doesn't have to exist, None should be returned by default
        fs.getxattr("/","testing-xattr")
        return fs
    except (AttributeError,UnsupportedError):
        return SimulateXAttr(fs)


class SimulateXAttr(WrapFS):
    """FS wrapper class that simulates xattr support.

    The following methods are supplied for manipulating extended attributes:
        * listxattrs:    list all extended attribute names for a path
        * getxattr:  get an xattr of a path by name
        * setxattr:  set an xattr of a path by name
        * delxattr:  delete an xattr of a path by name

    For each file in the underlying FS, this class maintains a corresponding 
    '.xattrs.FILENAME' file containing its extended attributes.  Extended
    attributes of a directory are stored in the file '.xattrs' within the
    directory itself.
    """

    def _get_attr_path(self, path, isdir=None):
        """Get the path of the file containing xattrs for the given path."""
        if isdir is None:
            isdir = self.wrapped_fs.isdir(path)
        if isdir:
            attr_path = pathjoin(path, '.xattrs')
        else:
            dir_path, file_name = pathsplit(path)
            attr_path = pathjoin(dir_path, '.xattrs.'+file_name)
        return attr_path

    def _is_attr_path(self, path):
        """Check whether the given path references an xattrs file."""
        _,name = pathsplit(path)
        if name.startswith(".xattrs"):
            return True
        return False

    def _get_attr_dict(self, path):
        """Retrieve the xattr dictionary for the given path."""
        attr_path = self._get_attr_path(path)
        if self.wrapped_fs.exists(attr_path):
            try:
                return pickle.loads(self.wrapped_fs.getcontents(attr_path))
            except EOFError:
                return {}
        else:
            return {}

    def _set_attr_dict(self, path, attrs):
        """Store the xattr dictionary for the given path."""
        attr_path = self._get_attr_path(path)
        self.wrapped_fs.setcontents(attr_path, pickle.dumps(attrs))

    @synchronize
    def setxattr(self, path, key, value):
        """Set an extended attribute on the given path."""
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        key = unicode(key)
        attrs = self._get_attr_dict(path)
        attrs[key] = str(value)
        self._set_attr_dict(path, attrs)

    @synchronize
    def getxattr(self, path, key, default=None):
        """Retrieve an extended attribute for the given path."""
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        attrs = self._get_attr_dict(path)
        return attrs.get(key, default)

    @synchronize
    def delxattr(self, path, key):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        attrs = self._get_attr_dict(path)
        try:
            del attrs[key]
        except KeyError:
            pass
        self._set_attr_dict(path, attrs)

    @synchronize
    def listxattrs(self,path):
        """List all the extended attribute keys set on the given path."""
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        return self._get_attr_dict(path).keys()

    def _encode(self,path):
        """Prevent requests for operations on .xattr files."""
        if self._is_attr_path(path):
            raise PathError(path,msg="Paths cannot contain '.xattrs': %(path)s")
        return path

    def _decode(self,path):
        return path

    def listdir(self,path="",*args,**kwds):
        """Prevent .xattr from appearing in listings."""
        entries = self.wrapped_fs.listdir(path,*args,**kwds)
        return [e for e in entries if not self._is_attr_path(e)]

    def ilistdir(self,path="",*args,**kwds):
        """Prevent .xattr from appearing in listings."""
        for e in self.wrapped_fs.ilistdir(path,*args,**kwds):
            if not self._is_attr_path(e):
                yield e

    def remove(self,path):
        """Remove .xattr when removing a file."""
        attr_file = self._get_attr_path(path,isdir=False)
        self.wrapped_fs.remove(path)
        try:
            self.wrapped_fs.remove(attr_file)
        except ResourceNotFoundError:
            pass

    def removedir(self,path,recursive=False,force=False):
        """Remove .xattr when removing a directory."""
        try:
            self.wrapped_fs.removedir(path,recursive=recursive,force=force)
        except DirectoryNotEmptyError:
            #  The xattr file could block the underlying removedir().
            #  Remove it, but be prepared to restore it on error.
            if self.listdir(path) != []:
                raise
            attr_file = self._get_attr_path(path,isdir=True)
            attr_file_contents = self.wrapped_fs.getcontents(attr_file)
            self.wrapped_fs.remove(attr_file)
            try:
                self.wrapped_fs.removedir(path,recursive=recursive)
            except FSError:
                self.wrapped_fs.setcontents(attr_file,attr_file_contents)
                raise

    def copy(self,src,dst,**kwds):
        """Ensure xattrs are copied when copying a file."""
        self.wrapped_fs.copy(self._encode(src),self._encode(dst),**kwds)
        s_attr_file = self._get_attr_path(src)
        d_attr_file = self._get_attr_path(dst)
        try:
            self.wrapped_fs.copy(s_attr_file,d_attr_file,overwrite=True)
        except ResourceNotFoundError,e:
            pass

    def move(self,src,dst,**kwds):
        """Ensure xattrs are preserved when moving a file."""
        self.wrapped_fs.move(self._encode(src),self._encode(dst),**kwds)
        s_attr_file = self._get_attr_path(src)
        d_attr_file = self._get_attr_path(dst)
        try:
            self.wrapped_fs.move(s_attr_file,d_attr_file,overwrite=True)
        except ResourceNotFoundError:
            pass

 
