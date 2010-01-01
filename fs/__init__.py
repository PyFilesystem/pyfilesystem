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

__version__ = "0.2.0a9"
__author__ = "Will McGugan (will@willmcgugan.com)"

#  'base' imports * from 'path' and 'errors', so their
#  contents will be available here as well.
from base import *

#  provide these by default so people can use 'fs.path.basename' etc.
import errors
import path

_thread_syncronize_default = True
def set_thread_syncronize_default(sync):
    """Sets the default thread synctonisation flag.
    
    FS objects are made thread-safe through the use of a per-FS threading Lock
    object. Since this can introduce an small overhead it can be disabled with
    this function if the code is single-threaded.
    
    :param sync: Set wether to use thread syncronization for new FS objects
    
    """
    global _thread_syncronization_default
    _thread_syncronization_default = sync

# Store some identifiers in the fs namespace
import os
SEEK_CUR = os.SEEK_CUR
SEEK_END = os.SEEK_END
SEEK_SET = os.SEEK_SET