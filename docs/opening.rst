Opening Filesystems
===================

Generally, when you want to work with the files and directories of any of the supported filesystems,
you create an instance of the appropriate class. For example, the following opens the directory ``/foo/bar``::

	from fs.osfs import OSFS
	my_fs = OSFS('/foo/bar')

This is fine if you know beforehand where the directory you want to work with is located, and on what medium.
However, there are occasions where the location of the files may change at runtime or should be specified in a config file or from the command line.

In these situations you can use an *opener*, which is a generic way of specifying a filesystem. For example, the following is equivalent to the code above::
	
	from fs.opener import fsopendir
	my_fs = fsopendir('/foo/bar')

The ``fsopendir`` callable takes a string that identifies the filesystem with a URI syntax, but if called with a regular path will return an :class:`~fs.osfs.OSFS` instance.
To open a different kind of filesystem, precede the path with the required protocol.
For example, the following code opens an FTP filesystem rather than a directory on your hard-drive::

	from fs.opener import fsopendir
	my_fs = fsopendir('ftp://example.org/foo/bar')

For further information regarding filesystem openers see :doc:`opener`.
