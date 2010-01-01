Filesystem Interface
====================

It requires a relatively small number of methods to implement a working Filesystem object.


Essential Methods
-----------------

The following methods are required for a minimal Filesystem interface:

    * `open` Opens a file for read/writing
    * `isfile` Check wether the path exists and is a file
    * `isdir` Check wether a path exists and is a directory
    * `listdir` List the contents of a directory
    * `makedir` Create a new directory
    * `remove` Remove an existing file
    * `removedir` Remove an existing directory
    * `rename` Automically rename a file or directory
    * `getinfo` Return information about the path e.h. size, mtime
    

Non - Essential Methods
-----------------------

The following methods have default implementations in fs.base.FS and aren't required for a functional FS interface. They may be overriden if an alternative implementation can be supplied:

    * `copy` Copy a file to a new location
    * `copydir` Recursively copy a directory to a new location
    * `desc` Return a short destriptive text regarding a path
    * `exists` Check whether a path exists as file or directory    
    * `getsyspath` Get a file's name in the local filesystem, if possible
    * `hassyspath` Check if a path maps to a system path (recognised by the OS)
    * `move` Move a file to a new location        
    * `movedir` Recursively move a directory to a new location
    * `opendir` Opens a directory and returns an FS object that represents it
    * `safeopen` Like `open` but returns a NullFile if the file could not be opened


Utility Methods
---------------

The following members have implementations in fs.base.FS but will probably never need a non-default implementation, although there is nothing to prevent a derived class from implementing these:

    * `createfile` Create a file with data
    * `getcontents` Returns the contents of a file as a string
    * `getsize` Returns the number of bytes used for a given file or directory
    * `isdirempty` Checks if a directory contains no files
    * `makeopendir` Creates a directroy (if it exists) and returns an FS object for that directory
    * `walk` Like `listdir` but descends in to sub-directories
    * `walkfiles` Returns an iterable of file paths in a directory, and its sub-directories
    * `walkdirs` Returns an iterable of paths to sub-directories