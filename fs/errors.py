"""

  fs.errors:  error class definitions for FS

"""

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
    """Exception raised when operation encounter remote connection trouble."""
    default_message = "Unable to %(opname)s: remote connection errror"



class ResourceError(FSError):
    """Base exception class for error associated with a specific resource."""

    default_message = "Unspecified resource error: %(path)s"

    def __init__(self,path,**kwds):
        self.path = path
        super(ResourceError,self).__init__(**kwds)


class NoSysPathError(ResourceError):
    """Exception raised when there is no syspath for a given path."""
    default_message = "No mapping to OS filesystem: %(path)s"


class ResourceNotFoundError(ResourceError):
    """Exception raised when a required resource is not found."""
    default_message = "Resource not found: %(path)s"


class DirectoryNotFoundError(ResourceNotFoundError):
    """Exception raised when a required directory is not found."""
    default_message = "Directory not found: %(path)s"


class FileNotFoundError(ResourceNotFoundError):
    """Exception raised when a required file is not found."""
    default_message = "File not found: %(path)s"


class ResourceInvalidError(ResourceError):
    """Exception raised when a required file is not found."""
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
