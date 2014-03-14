"""

  fs:  a filesystem abstraction.

This module provides an abstract base class 'FS' that defines a consistent
interface to different kinds of filesystem, along with a range of concrete
implementations of this interface such as:

    OSFS:       access the local filesystem, through the 'os' module
    TempFS:     a temporary filesystem that's automatically cleared on exit
    MemoryFS:   a filesystem that exists only in memory
    ZipFS:      access a zipfile like a filesystem
    SFTPFS:     access files on a SFTP server
    S3FS:       access files stored in Amazon S3

"""

__version__ = "0.5.0"
__author__ = "Will McGugan (will@willmcgugan.com)"

#  provide these by default so people can use 'fs.path.basename' etc.
from fs import errors
from fs import path

_thread_synchronize_default = True
def set_thread_synchronize_default(sync):
    """Sets the default thread synchronisation flag.

    FS objects are made thread-safe through the use of a per-FS threading Lock
    object. Since this can introduce an small overhead it can be disabled with
    this function if the code is single-threaded.

    :param sync: Set whether to use thread synchronisation for new FS objects

    """
    global _thread_synchronization_default
    _thread_synchronization_default = sync

# Store some identifiers in the fs namespace
import os
SEEK_CUR = os.SEEK_CUR
SEEK_END = os.SEEK_END
SEEK_SET = os.SEEK_SET


# Allow clean use of logging throughout the lib
import logging as _logging
class _NullHandler(_logging.Handler):
    def emit(self,record):
        pass
_logging.getLogger("fs").addHandler(_NullHandler())
def getLogger(name):
    """Get a logger object for use within the pyfilesystem library."""
    assert name.startswith("fs.")
    return _logging.getLogger(name)

