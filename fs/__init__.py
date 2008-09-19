"""
A filesystem abstraction.

"""

__version__ = "0.1dev"

__author__ = "Will McGugan (will@willmcgugan.com)"

from base import *
from helpers import *
__all__ = ['memoryfs',
           'mountfs',
           'multifs',
           'osfs',
           'utils',
           'zipfs',
           'helpers',
           'tempfs']