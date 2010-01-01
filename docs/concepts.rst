Concepts
========

It is generally quite easy to get in to the mind-set of using PyFilesystem interface over lower level interfaces -- since the code tends to be simpler -- but there are a few concepts which you will need to keep in mind.

Sandboxing
----------

FS objects are not permitted to work with any files / directories outside of the Filesystem they represent. If you attempt to open a file / directory outside the root of the FS (by using "../" in the path, you will get a ValueError).

It is advisable to write functions that takes FS objects as parameters, possibly with an additional path relative to the root of the FS. This allows you to write code that works with files / directories, but is independant of where they are located.


Paths
-----

Paths used within a Filesystem object use the same common format, regardless of the underlaying Filesystem it represents.

 * Path components are separated by a forward path (/)
 * Paths beginning with a forward slash are absolute (start at the root of the FS)
 * Paths not beginning with a forward slash are relative
 * A single dot means 'current directory'
 * A double dot means 'previous directory'
 
Note that paths used by the FS interface will use this format, but the constructor or additional methods may not. Notably the osfs.OSFS constructor which requires an OS path.

