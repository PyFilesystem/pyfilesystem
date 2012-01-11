"""
fs.osfs.xattrs
==============

Extended-attribute support for OSFS

"""


import os
import sys
import errno

from fs.errors import *
from fs.path import *
from fs.base import FS

try:
    import xattr
except ImportError:
    xattr = None


if xattr is not None:

    class OSFSXAttrMixin(object):
        """Mixin providing extended-attribute support via the 'xattr' module"""

        def __init__(self, *args, **kwargs):
            super(OSFSXAttrMixin, self).__init__(*args, **kwargs)

        @convert_os_errors
        def setxattr(self, path, key, value):
            xattr.xattr(self.getsyspath(path))[key]=value

        @convert_os_errors
        def getxattr(self, path, key, default=None):
            try:
                return xattr.xattr(self.getsyspath(path)).get(key)
            except KeyError:
                return default

        @convert_os_errors
        def delxattr(self, path, key):
            try:
                del xattr.xattr(self.getsyspath(path))[key]
            except KeyError:
                pass

        @convert_os_errors
        def listxattrs(self, path):
            return xattr.xattr(self.getsyspath(path)).keys()

else:

    class OSFSXAttrMixin(object):
        """Mixin disable extended-attribute support."""

        def __init__(self, *args, **kwargs):
            super(OSFSXAttrMixin, self).__init__(*args, **kwargs)

        def getxattr(self,path,key,default=None):
            raise UnsupportedError

        def setxattr(self,path,key,value):
            raise UnsupportedError

        def delxattr(self,path,key):
            raise UnsupportedError

        def listxattrs(self,path):
            raise UnsupportedError

