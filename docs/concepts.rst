Concepts
========

It is generally quite easy to get in to the mind-set of using PyFilesystem interface over lower level interfaces (since the code tends to be simpler) but there are a few concepts which you will need to keep in mind.

Sandboxing
----------

FS objects are not permitted to work with any files / directories outside of the Filesystem they represent. If you attempt to open a file or directory outside the root of the FS (e.g. by using "../" in the path) you will get a ValueError.

There is no concept of a current working directory in PyFilesystem, since it is a common source of bugs and not all filesytems even have such a notion. If you want to work with a sub-directory of a FS object, you can use the `opendir` method which returns another FS object representing the sub-directory.

For example, consider the following directory structure. The directory `foo` contains two sub-directories; `bar` and `baz`::

	 --foo
	   |--bar
	   |  |--readme.txt
	   |  `--photo.jpg
	   `--baz
	      |--private.txt
	      `--dontopen.jpg

We can open the `foo` directory with the following code::

	from fs.osfs import OSFS
	foo_fs = OSFS('foo')

The `foo_fs` object can work with any of the contents of `bar` and `baz`, which may not be desirable, especially if we are passing `foo_fs` to an untrusted function or one that could potentially delete files. Fortunately we can isolate a single sub-directory with then `opendir` method::

	bar_fs = foo_fs.opendir('bar')

This creates a completely new FS object that represents everything in the `foo/bar` directory. The root directory of `bar_fs` has been re-position, so that from `bar_fs`'s point of view, the readment.txt and photo.jpg files are in the root::

	--bar
	  |--readme.txt
	  `--photo.jpg

PyFilesystem will catch any attempts to read outside of the root directory. For example, the following will not work::

	bar_fs.open('../private.txt') # throws a ValueError


Paths
-----

Paths used within an FS object use the same common format, regardless of the underlaying file system it represents (or the platform it resides on). 

When working with paths in FS objects, keep in mind the following:

 * Path components are separated by a forward slash (/)
 * Paths beginning with a forward slash are absolute (start at the root of the FS)
 * Paths not beginning with a forward slash are relative
 * A single dot means 'current directory'
 * A double dot means 'previous directory'
 
Note that paths used by the FS interface will use this format, but the constructor or additional methods may not. Notably the ``osfs.OSFS`` constructor which requires an OS path -- the format of which can be platform-dependant.

There are many helpful functions for working with paths in the :mod:`fs.path` module.

System Paths
++++++++++++

Not all Python modules can use file-like objects, especially those which interface with C libraries. For these situations you will need to retrieve the `system path` from an FS object you are working with. You can do this with the `getsyspath` method which converts a valid path in the context of the FS object to an absolute path on the system, if one exists.

For example::

	>>> from fs.osfs import OSFS
	>>> home_fs = OSFS('~/')
	>>> home_fs.getsyspath('test.txt')
	u'/home/will/test.txt'

Not all FS implementation will map to a valid system path (e.g. the FTP FS object). If you call `getsyspath` on such FS objects you will either get a `NoSysPathError` exception or a return value of None, if you call `getsyspath` with `allow_none=True`.

Errors
------

PyFilesystem converts all exceptions to a common type, so that you need only write your exception handling code once. For example, if you try to open a file that doesn't exist, PyFilesystem will throw a ``fs.errors.ResourceNotFoundError`` regardless of whether the filesystem is local, on a ftp server or in a zip file::

	>>> from fs.osfs import OSFS
	>>> root_fs = OSFS('/')
	>>> root_fs.open('doesnotexist.txt')
	Traceback (most recent call last):
	  File "<stdin>", line 1, in <module>
	  File "/usr/local/lib/python2.6/dist-packages/fs/errors.py", line 181, in wrapper
	    return func(self,*args,**kwds)
	  File "/usr/local/lib/python2.6/dist-packages/fs/osfs/__init__.py", line 107, in open
	    return open(self.getsyspath(path), mode, kwargs.get("buffering", -1))
	fs.errors.ResourceNotFoundError: Resource not found: doesnotexist.txt

All PyFilesystem exceptions are derived from :class:`fs.errors.FSError`, so you may use that if you want to catch all possible exceptions.
