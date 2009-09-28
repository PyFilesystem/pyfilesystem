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

__version__ = "0.2.0a5"
__author__ = "Will McGugan (will@willmcgugan.com)"

#  'base' imports * from 'path' and 'errors', so their
#  contents will be available here as well.
from base import *

#  provide these by default so people can use 'fs.path.basename' etc.
import errors
import path


