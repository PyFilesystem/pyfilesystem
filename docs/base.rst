fs.base
=======

This module contains the basic FS interface and a number of other essential interfaces.

fs.base.FS
----------

All Filesystem objects inherit from this class,

.. autoclass:: fs.base.FS
    :members:
    
fs.base.SubFS
-------------

A SubFS is an FS implementation that represents a directory on another Filesystem. When you use the `opendir` method it will return a SubFS instance. You should not need to instantiate a SubFS directly.

For example::

    from fs.osfs import OSFS
    home_fs = OSFS('foo')
    bar_fs = home_fs.opendir('bar')


fs.base.NullFile
----------------

A NullFile is a file-like object with no functionality. It is used in situations where a file-like object is required but the caller doesn't have any data to read or write.

The `safeopen` method returns an NullFile instance, which can reduce error-handling code.

For example, the following code may be written to append some text to a log file::

    logfile = None
    try:
        logfile = myfs.open('log.txt', 'a')
        logfile.writeline('operation successful!')
    finally:
        if logfile is not None:
            logfile.close()

This could be re-written using the `safeopen` method::

    myfs.safeopen('log.txt', 'a').writeline('operation successful!')

If the file doesn't exist then the call to writeline will be a null-operation (i.e. not do anything)

