Getting Started
===============

PyFilesystem is a Python-only module and can be installed with easy_install or by source. PyFilesystem is know to work on Linux, Mac and OSX.

Installing
----------

The easiest way to install PyFilesystem is with `easy_install <http://peak.telecommunity.com/DevCenter/EasyInstall>`_::

    easy_install fs

This will install the latest stable release. If you would prefer to install the cutting edge release then you can get the latest copy of the source via SVN::

    svn checkout http://pyfilesystem.googlecode.com/svn/trunk/ pyfilesystem-read-only
    cd pyfilesystem-read-only
    python setup.py install

You should now have the `fs` module on your path:

    >>> import fs
    >>> fs.__version__
    '0.2.0a9'

Prerequisites
-------------

PyFilesystem requires at least **Python 2.4**. There are a few other dependancies if you want to use some of the more advanced filesystem interfaces, but for basic use all that is needed is the Python standard library.

    * wxPython (required for fs.browsewin) http://www.wxpython.org/
    * Boto (required for fs.s3fs) http://code.google.com/p/boto/
    * Paramikio (required for fs.ftpfs) http://www.lag.net/paramiko/    


Quick Examples
--------------

Before you dive in to the API documentation, here are a few interesting things you can do with pyFilesystem.

The following will list all the files in your home directory::

    >>> from fs.osfs import OSFS
    >>> home_fs = OSFS('~/') # 'c:\Users\<login name>' on Windows
    >>> home_fs.listdir()
    
This will display the total number of bytes store in '.py' files your home directory::

    >>> sum(home_fs.getsize(f) for f in home_fs.walkfiles(wildcard='*.py'))
