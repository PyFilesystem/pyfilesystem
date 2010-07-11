"""A version of wraps for Python versions that don't support it"""

try:
    from functools import wraps as wraps
except ImportError:
    wraps = lambda f: lambda f: f