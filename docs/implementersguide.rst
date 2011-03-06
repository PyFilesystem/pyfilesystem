A Guide For Filesystem Implementers 
===================================

PyFilesystems objects are designed to be as generic as possible and still expose as much functionality as possible.
With a little care, you can write a wrapper for your filesystem that allows it to work interchangeably with any of the built-in FS classes and tools. 

To create a working PyFilesystem interface, derive a class from :py:class:`fs.base.FS` and implement the 9 :ref:`essential-methods`.


Filesystem Errors
-----------------

With the exception of the constuctor, FS methods should throw :class:`fs.errors.FSError` exceptions in preference to any specific exception classes,
so that generic exception handling can be written.
The constructor *may* throw a non-FSError exception, if no appropriate FSError exists.
The rational for this is that creating an FS interface may require specific knowledge,
but this shouldn't prevent it from working with more generic code.

If specific exceptions need to be translated in to an equivalent FSError,
pass the original exception class to the FSError constructor with the 'details' keyword argument.

For example, the following translates some ficticious exception in to an FS exception,
and passes the original exception as an argument.::

    try:
        someapi.open(path, mode)
    except someapi.UnableToOpen, e:
        raise errors.ResourceNotFoundError(path=path, details=e)
		
Any code written to catch the generic error, can also retrieve the original exception if it contains additional information. 