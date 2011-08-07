Getting Started
===============

PyFilesystem is a Python-only module and can be installed with easy_install or from source. PyFilesystem is known to work on Linux, Mac and OSX.

Installing
----------

The easiest way to install PyFilesystem is with `easy_install <http://peak.telecommunity.com/DevCenter/EasyInstall>`_::

    easy_install fs

Add the -U switch if you want to upgrade a previous installation::

	easy_install -U fs
	
If you prefer to use Pip (http://pypi.python.org/pypi/pip) to install Python packages, the procedure is much the same::

	pip install fs
	
Or to upgrade::

	pip install fs --upgrade

You can also install the cutting edge release by checking out the source via SVN::

    svn checkout http://pyfilesystem.googlecode.com/svn/trunk/ pyfilesystem-read-only
    cd pyfilesystem-read-only
    python setup.py install

Whichever method you use, you should now have the `fs` module on your path (version number may vary)::

    >>> import fs
    >>> fs.__version__
    '0.4.0'

Prerequisites
-------------

PyFilesystem requires at least **Python 2.5**. There are a few other dependencies if you want to use some of the more advanced filesystem interfaces, but for basic use all that is needed is the Python standard library.
    
    * Boto (required for :mod:`fs.s3fs`) http://code.google.com/p/boto/
    * Paramiko (required for :class:`fs.ftpfs.FTPFS`) http://www.lag.net/paramiko/
    * wxPython (required for :mod:`fs.browsewin`) http://www.wxpython.org/    


Quick Examples
--------------

Before you dive in to the API documentation, here are a few interesting things you can do with PyFilesystem.

The following will list all the files in your home directory::

    >>> from fs.osfs import OSFS
    >>> home_fs = OSFS('~/') # 'c:\Users\<login name>' on Windows
    >>> home_fs.listdir()
    
Here's how to browse your home folder with a graphical interface::
    
	>>> home_fs.browse()
    
This will display the total number of bytes store in '.py' files your home directory::

    >>> sum(home_fs.getsize(f) for f in home_fs.walkfiles(wildcard='*.py'))
