Introduction
============

PyFilesystem is a Python module that provides a common interface to any filesystem.

Think of PyFilesystem (FS) objects as the next logical step to Python's `file` objects. Just as file-like objects abstract a single file, FS objects abstract the whole filesystem by providing a common interface to operations such as reading directories, getting file information, opening/copying/deleting files etc.

Even if you only want to work with the local filesystem, PyFilesystem simplifies a number of common operations and reduces the chance of error.

