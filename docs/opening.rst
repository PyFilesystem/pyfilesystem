Opening Filesystems
===================

Generally, when you want to work with the files and directories of any of the supported filesytems,
you create an instance of the appropriate class. For example, the following opens the directory /foo/bar::

	from fs.osfs import OSFS
	my_fs = OSFS('/foo/bar')

This is fine if you know beforehand where the directory you want to work with is located, and on what medium.
However, there are occasions where the location of the files may change at runtime or should be specified in a config file or from the command line.

In these situations you can use an _opener_ which is a generic way of specifying a filesystem. For example, the following is equivalent to the code above::
	
	from fs.opener import fsopen
	my_fs = fsopen('/foo/bar')

The `fsopen` method takes a string that identifies the filesystem, but if called with a regular path, it will return an OSFS instance.
To open a different kind of filesystem, you specify it with a URI like syntax. The following code opens an ftp filesystem rather than a directory on your harddrive::

	from fs.opener import fsopen
	my_fs = fsopen('ftp://example.org/foo/bar')

For further information regarding filesystem openers see :doc:`opener`.