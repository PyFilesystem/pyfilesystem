"""

  A version of functools.wraps for Python versions that don't support it.

Note that this module can't be named "functools" because it would shadow the
stdlib module that it tries to emulate.  Absolute imports would fix this
problem but are only availabe from Python 2.5.

"""

try:
    from functools import wraps as wraps
except ImportError:
    wraps = lambda f: lambda f: f
