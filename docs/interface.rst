.. _filesystem-interface:

Filesystem Interface
====================

The following methods are available in all PyFilesystem implementation:
	
	* :meth:`~fs.base.FS.close` Close the filesystem and free any resources
	* :meth:`~fs.base.FS.copy` Copy a file to a new location
	* :meth:`~fs.base.FS.copydir` Recursively copy a directory to a new location
	* :meth:`~fs.base.FS.cachehint` Permit implementation to use aggressive caching for performance reasons
	* :meth:`~fs.base.FS.createfile` Create a file with data
	* :meth:`~fs.base.FS.desc` Return a short descriptive text regarding a path
	* :meth:`~fs.base.FS.exists` Check whether a path exists as file or directory
	* :meth:`~fs.base.FS.getcontents` Returns the contents of a file as a string
	* :meth:`~fs.base.FS.getinfo` Return information about the path e.g. size, mtime
	* :meth:`~fs.base.FS.getmeta` Get the value of a filesystem meta value, if it exists
	* :meth:`~fs.base.FS.getmmap` Gets an mmap object for the given resource, if supported
	* :meth:`~fs.base.FS.getpathurl` Get an external URL at which the given file can be accessed, if possible
	* :meth:`~fs.base.FS.getsize` Returns the number of bytes used for a given file or directory
	* :meth:`~fs.base.FS.getsyspath` Get a file's name in the local filesystem, if possible
	* :meth:`~fs.base.FS.hasmeta` Check if a filesystem meta value exists
	* :meth:`~fs.base.FS.haspathurl` Check if a path maps to an external URL
	* :meth:`~fs.base.FS.hassyspath` Check if a path maps to a system path (recognized by the OS)
	* :meth:`~fs.base.FS.ilistdir` Generator version of the :meth:`~fs.base.FS.listdir` method
	* :meth:`~fs.base.FS.ilistdirinfo` Generator version of the :meth:`~fs.base.FS.listdirinfo` method
	* :meth:`~fs.base.FS.isdir` Check whether a path exists and is a directory
	* :meth:`~fs.base.FS.isdirempty` Checks if a directory contains no files
	* :meth:`~fs.base.FS.isfile` Check whether the path exists and is a file
	* :meth:`~fs.base.FS.listdir` List the contents of a directory
	* :meth:`~fs.base.FS.listdirinfo` Get a directory listing along with the info dict for each entry
	* :meth:`~fs.base.FS.makedir` Create a new directory
	* :meth:`~fs.base.FS.makeopendir` Make a directory and returns the FS object that represents it
	* :meth:`~fs.base.FS.move` Move a file to a new location
	* :meth:`~fs.base.FS.movedir` Recursively move a directory to a new location
	* :meth:`~fs.base.FS.open` Opens a file for read/writing
	* :meth:`~fs.base.FS.opendir` Opens a directory and returns a FS object that represents it
	* :meth:`~fs.base.FS.remove` Remove an existing file
	* :meth:`~fs.base.FS.removedir` Remove an existing directory
	* :meth:`~fs.base.FS.rename` Atomically rename a file or directory
	* :meth:`~fs.base.FS.safeopen` Like :meth:`~fs.base.FS.open` but returns a :class:`~fs.base.NullFile` if the file could not be opened
	* :meth:`~fs.base.FS.setcontents` Sets the contents of a file as a string or file-like object
	* :meth:`~fs.base.FS.setcontents_async` Sets the contents of a file asynchronously
	* :meth:`~fs.base.FS.settimes` Sets the accessed and modified times of a path
	* :meth:`~fs.base.FS.tree` Display an ascii rendering of the directory structure
	* :meth:`~fs.base.FS.walk` Like :meth:`~fs.base.FS.listdir` but descends in to sub-directories
	* :meth:`~fs.base.FS.walkdirs` Returns an iterable of paths to sub-directories
	* :meth:`~fs.base.FS.walkfiles` Returns an iterable of file paths in a directory, and its sub-directories

See :py:class:`fs.base.FS` for the method signature and full details.

If you intend to implement an FS object, see :ref:`implementers`.
