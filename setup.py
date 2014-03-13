#!/usr/bin/env python

#from distribute_setup import use_setuptools
#use_setuptools()

from setuptools import setup
import sys
PY3 = sys.version_info >= (3,)

VERSION = "0.5.0"

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
    "Development Status :: 5 - Production/Stable",
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Topic :: System :: Filesystems',
]

with open('README.txt', 'r') as f:
    long_desc = f.read()


extra = {}
if PY3:
    extra["use_2to3"] = True

setup(install_requires=['distribute', 'six'],
      name='fs',
      version=VERSION,
      description="Filesystem abstraction layer",
      long_description=long_desc,
      license="BSD",
      author="Will McGugan",
      author_email="will@willmcgugan.com",
      #url="http://code.google.com/p/pyfilesystem/",
      #download_url="http://code.google.com/p/pyfilesystem/downloads/list",
      url="http://pypi.python.org/pypi/fs/"
      platforms=['any'],
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
      package_data={'fs': ['tests/data/*.txt']},
      scripts=['fs/commands/%s' % command for command in COMMANDS],
      classifiers=classifiers,
      **extra
      )
