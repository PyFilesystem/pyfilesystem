"""

  fs.errors:  error class definitions for FS

"""

import sys
import errno

from fs.path import *

try:
    from functools import wraps
except ImportError:
    def wraps(func):
        def decorator(wfunc):
            wfunc.__name__ == func.__name__
            wfunc.__doc__ == func.__doc__
            wfunc.__module__ == func.__module__
        return decorator


class FSError(Exception):
    """Base exception class for the FS module."""
    default_message = "Unspecified error"

    def __init__(self,msg=None,details=None):
        if msg is None:
            msg = self.default_message
        self.msg = msg
        self.details = details

    def __str__(self):
        keys = dict((k,str(v)) for k,v in self.__dict__.iteritems())
        return self.msg % keys

    def __unicode__(self):
        return unicode(str(self))


class PathError(FSError):
    """Exception for errors to do with a path string."""
    default_message = "Path is invalid: %(path)s"

    def __init__(self,path,**kwds):
        self.path = path
        super(PathError,self).__init__(**kwds)
 

class OperationFailedError(FSError):
    """Base exception class for errors associated with a specific operation."""
    default_message = "Unable to %(opname)s: unspecified error [%(errno)s - %(details)s]"

    def __init__(self,opname,path=None,**kwds):
        self.opname = opname
        self.path = path
        self.errno = getattr(kwds.get("details",None),"errno",None)
        super(OperationFailedError,self).__init__(**kwds)


class UnsupportedError(OperationFailedError):
    """Exception raised for operations that are not supported by the FS."""
    default_message = "Unable to %(opname)s: not supported by this filesystem"


class RemoteConnectionError(OperationFailedError):
    """Exception raised when operations encounter remote connection trouble."""
    default_message = "Unable to %(opname)s: remote connection errror"


class StorageSpaceError(OperationFailedError):
    """Exception raised when operations encounter storage space trouble."""
    default_message = "Unable to %(opname)s: insufficient storage space"


class PermissionDeniedError(OperationFailedError):
    default_message = "Unable to %(opname)s: permission denied"



class ResourceError(FSError):
    """Base exception class for error associated with a specific resource."""
    default_message = "Unspecified resource error: %(path)s"

    def __init__(self,path,**kwds):
        self.path = path
        self.opname = kwds.pop("opname",None)
        super(ResourceError,self).__init__(**kwds)


class NoSysPathError(ResourceError):
    """Exception raised when there is no syspath for a given path."""
    default_message = "No mapping to OS filesystem: %(path)s"


class ResourceNotFoundError(ResourceError):
    """Exception raised when a required resource is not found."""
    default_message = "Resource not found: %(path)s"


class ResourceInvalidError(ResourceError):
    """Exception raised when a resource is the wrong type."""
    default_message = "Resource is invalid: %(path)s"


class DestinationExistsError(ResourceError):
    """Exception raised when a target destination already exists."""
    default_message = "Destination exists: %(path)s"


class DirectoryNotEmptyError(ResourceError):
    """Exception raised when a directory to be removed is not empty."""
    default_message = "Directory is not empty: %(path)s"


class ParentDirectoryMissingError(ResourceError):
    """Exception raised when a parent directory is missing."""
    default_message = "Parent directory is missing: %(path)s"


class ResourceLockedError(ResourceError):
    """Exception raised when a resource can't be used because it is locked."""
    default_message = "Resource is locked: %(path)s"



def convert_fs_errors(func):
    """Function wrapper to convert FSError instances into OSErrors."""
    @wraps(func)
    def wrapper(*args,**kwds):
        try:
            return func(*args,**kwds)
        except ResourceNotFoundError, e:
            raise OSError(errno.ENOENT,str(e))
        except ResourceInvalidError, e:
            raise OSError(errno.EINVAL,str(e))
        except PermissionDeniedError, e:
            raise OSError(errno.EACCESS,str(e))
        except DirectoryNotEmptyError, e:
            raise OSError(errno.ENOTEMPTY,str(e))
        except DestinationExistsError, e:
            raise OSError(errno.EEXIST,str(e))
        except StorageSpaceError, e:
            raise OSError(errno.ENOSPC,str(e))
        except RemoteConnectionError, e:
            raise OSError(errno.ENONET,str(e))
        except UnsupportedError, e:
            raise OSError(errno.ENOSYS,str(e))
        except FSError, e:
            raise OSError(errno.EFAULT,str(e))
    return wrapper


def convert_os_errors(func):
    """Function wrapper to convert OSError/IOError instances into FSErrors."""
    opname = func.__name__
    @wraps(func)
    def wrapper(self,*args,**kwds):
        try:
            return func(self,*args,**kwds)
        except (OSError,IOError), e:
            path = getattr(e,"filename",None)
            if path and path[0] == "/" and hasattr(self,"root_path"):
                path = normpath(path)
                if isprefix(self.root_path,path):
                    path = path[len(self.root_path):]
            if not hasattr(e,"errno") or not e.errno:
                raise OperationFailedError(opname,details=e)
            if e.errno == errno.ENOENT:
                raise ResourceNotFoundError(path,opname=opname,details=e)
            if e.errno == errno.ENOTEMPTY:
                raise DirectoryNotEmptyError(path,opname=opname,details=e)
            if e.errno == errno.EEXIST:
                raise DestinationExistsError(path,opname=opname,details=e)
            if e.errno == 183: # some sort of win32 equivalent to EEXIST
                raise DestinationExistsError(path,opname=opname,details=e)
            if e.errno == errno.ENOTDIR:
                raise ResourceInvalidError(path,opname=opname,details=e)
            if e.errno == errno.EISDIR:
                raise ResourceInvalidError(path,opname=opname,details=e)
            if e.errno == errno.EINVAL:
                raise ResourceInvalidError(path,opname=opname,details=e)
            raise OperationFailedError(opname,details=e)
    return wrapper


