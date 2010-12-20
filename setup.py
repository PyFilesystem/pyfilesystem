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

setup(name='fs',
      version=VERSION,
      description="Filesystem abstraction",
      long_description="Creates a common interface to filesystems",
      license = "BSD",
      author="Will McGugan",
      author_email="will@willmcgugan.com",
      url="http://code.google.com/p/pyfilesystem/",
      download_url="http://code.google.com/p/pyfilesystem/downloads/list",
      platforms = ['any'],
      packages=['fs','fs.expose','fs.expose.fuse','fs.tests','fs.wrapfs',
                'fs.osfs','fs.contrib','fs.contrib.bigfs','fs.contrib.davfs',
                'fs.expose.dokan', 'fs.commands'],
      scripts=['fs/commands/%s.py' % command for command in COMMANDS],
      classifiers=classifiers,
      )

