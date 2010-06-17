Introduction
============

PyFilesystem is a Python module that provides a common interface to any filesystem.

Think of PyFilesystem (FS) objects as the next logical step to Python's `file` objects. Just as file-like objects abstract a single file, FS objects abstract the whole filesystem by providing a common interface to operations such as reading directories, getting file information, opening/copying/deleting files etc.

Even if you only want to work with the local filesystem, FS simplifies a number of common operations and reduces the chance of error. A typical problem when working with the filesystem is writing a function that changes the current working directory, but doesn't set it back. This can be a tricky bug to identify since the problem will only manifest itself when you next try to work with the filesystem. PyFilesytem doesn't have this problem, because it doesn't modify the current working directory. It is also possible to restrict any file operations to a specific directory, which elliminates the possibility of accidently overwriting / deleting files outside of the specified directory.

