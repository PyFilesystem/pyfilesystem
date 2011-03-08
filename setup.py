#!/usr/bin/env python

from distutils.core import setup
from fs import __version__ as VERSION

COMMANDS = ['fscat',
            'fscp',
            'fsinfo',
            'fsls',
            'fsmv',
            'fscp',
            'fsrm',
            'fsserve',
            'fstree',
            'fsmkdir',
            'fsmount']


classifiers = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: System :: Filesystems',
]

long_desc = """Pyfilesystem is a module that provides a simplified common interface to many types of filesystem. Filesystems exposed via Pyfilesystem can also be served over the network, or 'mounted' on the native filesystem.

Even if you only need to work with file and directories on the local hard-drive, Pyfilesystem can simplify your code and make it more robust -- with the added advantage that you can change where the files are located by changing a single line of code.
"""

setup(name='fs',
      version=VERSION,
      description="Filesystem abstraction",
      long_description=long_desc,
      license = "BSD",
      author="Will McGugan",
      author_email="will@willmcgugan.com",
      url="http://code.google.com/p/pyfilesystem/",
      download_url="http://code.google.com/p/pyfilesystem/downloads/list",
      platforms = ['any'],
      packages=['fs',
                'fs.expose',
                'fs.expose.dokan',
                'fs.expose.fuse',
                'fs.expose.wsgi',
                'fs.tests',
                'fs.wrapfs',
                'fs.osfs',
                'fs.contrib',
                'fs.contrib.bigfs',
                'fs.contrib.davfs',
                'fs.contrib.tahoelafs',                
                'fs.commands'],
      scripts=['fs/commands/%s' % command for command in COMMANDS],
      classifiers=classifiers,
      )

