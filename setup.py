#!/user/bin/env python

from distutils.core import setup

from fs import __version__ as VERSION

classifiers = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Python Software Foundation License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: System :: Filesystems',
]

setup(name='fs',
      version=VERSION,
      description="Filesystem abstraction",
      long_description="Creates a common interface to filesystems",
      license = "Python Software Foundation License",
      author="Will McGugan",
      author_email="will@willmcgugan.com",
      url="http://code.google.com/p/pyfilesystem/",
      download_url="http://code.google.com/p/pyfilesystem/downloads/list",
      platforms = ['any'],
      packages=['fs'],
      classifiers=classifiers,
      )
