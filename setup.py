#!/usr/bin/env python

from setuptools import setup
import sys
PY3 = sys.version_info >= (3,)

VERSION = "0.5.1"

COMMANDS = ['fscat',
            'fsinfo',
            'fsls',
            'fsmv',
            'fscp',
            'fsrm',
            'fsserve',
            'fstree',
            'fsmkdir',
            'fsmount']


CONSOLE_SCRIPTS = ['{0} = fs.commands.{0}:run'.format(command)
                   for command in COMMANDS]

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

setup(install_requires=['setuptools', 'six'],
      name='fs',
      version=VERSION,
      description="Filesystem abstraction layer",
      long_description=long_desc,
      license="BSD",
      author="Will McGugan",
      author_email="will@willmcgugan.com",
      url="http://pypi.python.org/pypi/fs/",
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
      entry_points={"console_scripts": CONSOLE_SCRIPTS},
      classifiers=classifiers,
      **extra
      )
