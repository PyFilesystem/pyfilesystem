PyFilesystem
============

PyFilesystem is an abstraction layer for *filesystems*. In the same way that Python's file-like objects provide a common way of accessing files, PyFilesystem provides a common way of accessing entire filesystems. You can write platform-independent code to work with local files, that also works with any of the supported filesystems (zip, ftp, S3 etc.).

Pyfilesystem works with Linux, Windows and Mac.

Supported Filesystems
---------------------

Here are a few of the filesystems that can be accessed with Pyfilesystem:

* **DavFS** access files & directories on a WebDAV server
* **FTPFS** access files & directories on an FTP server
* **MemoryFS** access files & directories stored in memory (non-permanent but very fast)
* **MountFS** creates a virtual directory structure built from other filesystems
* **MultiFS** a virtual filesystem that combines a list of filesystems into one, and checks them in order when opening files
* **OSFS** the native filesystem
* **SFTPFS** access files & directories stored on a Secure FTP server
* **S3FS** access files & directories stored on Amazon S3 storage
* **TahoeLAFS** access files & directories stored on a Tahoe distributed filesystem
* **ZipFS** access files and directories contained in a zip file

Example
-------

The following snippet prints the total number of bytes contained in all your Python files in `C:/projects` (including sub-directories)::

    from fs.osfs import OSFS
    projects_fs = OSFS('C:/projects')
    print sum(projects_fs.getsize(path)
              for path in projects_fs.walkfiles(wildcard="*.py"))

That is, assuming you are on Windows and have a directory called 'projects' in your C drive. If you are on Linux / Mac, you might replace the second line with something like::

    projects_fs = OSFS('~/projects')

If you later want to display the total size of Python files stored in a zip file, you could make the following change to the first two lines::

    from fs.zipfs import ZipFS
    projects_fs = ZipFS('source.zip')

In fact, you could use any of the supported filesystems above, and the code would continue to work as before.

An alternative to explicitly importing the filesystem class you want, is to use an FS opener which opens a filesystem from a URL-like syntax::

    from fs.opener import fsopendir
    projects_fs = fsopendir('C:/projects')

You could change ``C:/projects`` to ``zip://source.zip`` to open the zip file, or even ``ftp://ftp.example.org/code/projects/`` to sum up the bytes of Python stored on an ftp server.

Documentation
-------------

http://docs.pyfilesystem.org

Screencast
----------

This is from an early version of PyFilesystem, but still relevant

http://vimeo.com/12680842

Discussion Group
----------------

http://groups.google.com/group/pyfilesystem-discussion

Further Information
-------------------

http://www.willmcgugan.com/tag/fs/
