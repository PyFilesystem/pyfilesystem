Introduction
============

PyFilesystem is a Python module that provides a common interface to disparate filesystems, which allows the developer to write code that works with files and directories regardless of their source and location.

Think of PyFilesystem (FS) objects as the next logical step to Python's _file_ objects. Just as file-like objects abstract a single file, FS objects abstract the whole filesystem by providing a common interface to operations such as reading directories, getting file information, opening/copying/deleting files etc.

For example, if you had written a method that reads a few files from the local filesystem, it would be a trivial change if you later decided to read those files from a zip file or even over the Internet.