"""

  fs.wrappers.xattr:  FS wrapper for simulating extended-attribute support.

"""

try:
    import cPickle as pickle
except ImportError:
    import pickle

from fs.helpers import *
from fs.errors import *
from fs.wrappers import FSWrapper

class SimulateXAttr(FSWrapper):
    """FS wrapper class that simulates xattr support.

    For each file in the underlying FS, this class managed a corresponding 
    '.xattr' file containing its extended attributes.
    """

    def _get_attr_path(self, path):
        """Get the path of the file containing xattrs for the given path."""
        if self.wrapped_fs.isdir(path):
            return pathjoin(path, '.xattrs.')
        else:
            dir_path, file_name = pathsplit(path)
            return pathjoin(dir_path, '.xattrs.'+file_name)

    def _is_attr_path(self, path):
        """Check whether the given path references an xattrs file."""
        _,name = pathsplit(path)
        if name.startswith(".xattrs."):
            return True
        return False

    def _get_attr_dict(self, path):
        """Retrieve the xattr dictionary for the given path."""
        attr_path = self._get_attr_path(path)
        if self.wrapped_fs.exists(attr_path):
            return pickle.loads(self.wrapped_fs.getcontents(attr_path))
        else:
            return {}

    def _set_attr_dict(self, path, attrs):
        """Store the xattr dictionary for the given path."""
        attr_path = self._get_attr_path(path)
        self.wrapped_fs.setcontents(self._get_attr_path(path), pickle.dumps(attrs))

    def setxattr(self, path, key, value):
        """Set an extended attribute on the given path."""
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        attrs = self._get_attr_dict(path)
        attrs[key] = value
        self._set_attr_dict(path, attrs)

    def getxattr(self, path, key, default=None):
        """Retrieve an extended attribute for the given path."""
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        attrs = self._get_attr_dict(path)
        return attrs.get(key, default)

    def delxattr(self, path, key):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        attrs = self._get_attr_dict(path)
        try:
            del attrs[key]
        except KeyError:
            pass
        self._set_attr_dict(path, attrs)

    def xattrs(self,path):
        """List all the extended attribute keys set on the given path."""
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        return self._get_attr_dict(path).keys()

    def _encode(self,path):
        """Prevent requests for operations on .xattr files."""
        if self._is_attr_path(path):
            raise PathError(path,msg="Paths cannot contain '.xattrs.': %(path)s")
        return path

    def _decode(self,path):
        return path

    def listdir(self,path="",**kwds):
        """Prevent .xattr from appearing in listings."""
        entries = self.wrapped_fs.listdir(path,**kwds)
        return [e for e in entries if not self._is_attr_path(e)]

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
 
