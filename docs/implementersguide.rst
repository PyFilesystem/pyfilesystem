.. _implementers:

A Guide For Filesystem Implementers 
===================================

PyFilesystems objects are designed to be as generic as possible and still expose the full filesystem functionality.
With a little care, you can write a wrapper for your filesystem that allows it to work interchangeably with any of the built-in FS classes and tools. 

To create a working PyFilesystem interface, derive a class from :py:class:`fs.base.FS` and implement the 9 :ref:`essential-methods`.
The base class uses these essential methods as a starting point for providing a lot of extra functionality,
but in some cases the default implementation may not be the most efficient.
For example, most filesystems have an atomic way of moving a file from one location to another without having to copy data,
whereas the default implementation of :meth:`~fs.base.FS.move` method must copy all the bytes in the source file to the destination file.
Any of the :ref:`non-essential-methods` may be overriden, but efficient custom versions of the following methods will have the greatest impact on performance:     

	* :meth:`~fs.base.FS.copy` copy a file
	* :meth:`~fs.base.FS.copydir` copy a directory
	* :meth:`~fs.base.FS.exists` check if a file / directory exists
	* :meth:`~fs.base.FS.getsyspath` get a system path for a given resource, if it exists 	
	* :meth:`~fs.base.FS.move` move a file
	* :meth:`~fs.base.FS.movedir` move a directory

For network based filesystems (i.e. where the physical data is pulled over a network),
there are a few methods which can reduce the number of round trips to the server,
if an efficient implementation is provided:
	
	* :meth:`~fs.base.FS.listdirinfo` returns the directory contents and info dictionary in one call
	* :meth:`~fs.base.FS.ilistdir` a generator version of :meth:`~fs.base.FS.listdir` 
	* :meth:`~fs.base.FS.ilistdirinfo` a generator version of :meth:`~fs.base.FS.listdirinfo`

The generator methods (beginning with ``i``) are intended for use with filesystems that contain a lot of files,
where reading the directory in one go may be expensive.

Other methods in the :doc:`interface` are unlikely to require a non-default implementation,
but there is nothing preventing you from implementing them -- just be careful to use the same signature and replicate expected functionality. 

Filesystem Errors
-----------------

With the exception of the constuctor, FS methods should throw :class:`fs.errors.FSError` exceptions in preference to any implementation-specific exception classes,
so that generic exception handling can be written.
The constructor *may* throw a non-FSError exception, if no appropriate FSError exists.
The rationale for this is that creating an FS interface may require specific knowledge,
but this shouldn't prevent it from working with more generic code.

If specific exceptions need to be translated in to an equivalent FSError,
pass the original exception class to the FSError constructor with the 'details' keyword argument.

For example, the following translates some fictitious exception in to an FSError exception,
and passes the original exception as an argument.::

    try:
        someapi.open(path, mode)
    except someapi.UnableToOpen, e:
        raise errors.ResourceNotFoundError(path=path, details=e)
		
Any code written to catch the generic error, can also retrieve the original exception if it contains additional information.

Thread Safety
-------------

All PyFilesystems methods, other than the constructor, should be thread-safe where-ever possible.
One way to do this is to pass ``threads_synchronize=True`` to the base constructor and use the :func:`~fs.base.synchronize` decorator to lock the FS object when a method is called.

If the implementation can not be made thread-safe for technical reasons, ensure that ``getmeta("thread_safe")`` returns ``False``.


Meta Values
-----------

The :meth:`~fs.base.FS.getmeta` method is designed to return implementation specific information.
PyFilesystem implementations should return as much of the standard set of meta values as possible.

Implementations are also free to reserve a dotted namespace notation for themselves, to provide an interface to highly specific information.
If you do this, please avoid generic terms as they may conflict with existing or future implementations.
For example ``"bobs_ftpfs.author"``, rather than ``"ftpfs.author"``.

If your meta values are static, i.e. they never change, then create a dictionary class attribute called ``_meta`` in your implementation that contains all the meta keys and values. 
The default ``getmeta`` implementation will pull the meta values from this dictionary.

.. _essential-methods:

Essential Methods
-----------------

The following methods are required for a minimal Filesystem interface:

    * :meth:`~fs.base.FS.open` Opens a file for read/writing
    * :meth:`~fs.base.FS.isfile` Check wether the path exists and is a file
    * :meth:`~fs.base.FS.isdir` Check wether a path exists and is a directory
    * :meth:`~fs.base.FS.listdir` List the contents of a directory
    * :meth:`~fs.base.FS.makedir` Create a new directory
    * :meth:`~fs.base.FS.remove` Remove an existing file
    * :meth:`~fs.base.FS.removedir` Remove an existing directory
    * :meth:`~fs.base.FS.rename` Atomically rename a file or directory
    * :meth:`~fs.base.FS.getinfo` Return information about the path e.g. size, mtime
    

.. _non-essential-methods:

Non - Essential Methods
-----------------------

The following methods have default implementations in :py:class:`fs.base.FS` and aren't required for a functional FS interface. They may be overriden if an alternative implementation can be supplied:

    * :meth:`~fs.base.FS.copy` Copy a file to a new location
    * :meth:`~fs.base.FS.copydir` Recursively copy a directory to a new location
    * :meth:`~fs.base.FS.desc` Return a short destriptive text regarding a path
    * :meth:`~fs.base.FS.exists` Check whether a path exists as file or directory    
    * :meth:`~fs.base.FS.listdirinfo` Get a directory listing along with the info dict for each entry
    * :meth:`~fs.base.FS.ilistdir` Generator version of the listdir method
    * :meth:`~fs.base.FS.ilistdirinfo` Generator version of the listdirinfo method
    * :meth:`~fs.base.FS.getpathurl` Get an external URL at which the given file can be accessed, if possible
    * :meth:`~fs.base.FS.getsyspath` Get a file's name in the local filesystem, if possible
    * :meth:`~fs.base.FS.getmeta` Get the value of a filesystem meta value, if it exists
    * :meth:`~fs.base.FS.getmmap` Gets an mmap object for the given resource, if supported
    * :meth:`~fs.base.FS.hassyspath` Check if a path maps to a system path (recognised by the OS)    
    * :meth:`~fs.base.FS.haspathurl` Check if a path maps to an external URL    
    * :meth:`~fs.base.FS.hasmeta` Check if a filesystem meta value exists
    * :meth:`~fs.base.FS.move` Move a file to a new location        
    * :meth:`~fs.base.FS.movedir` Recursively move a directory to a new location    
    * :meth:`~fs.base.FS.settimes` Sets the accessed and modified times of a path
