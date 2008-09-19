"""
A filesystem abstraction.

"""

__version__ = "0.1.0"

__author__ = "Will McGugan (will@willmcgugan.com)"

from fs import *
from helpers import *
__all__ = ['memoryfs',
           'mountfs',
           'multifs',
           'osfs',
           'utils',
           'zipfs',
           'helpers',
           'tempfs']