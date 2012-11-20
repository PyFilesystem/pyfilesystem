"""
fs.wrapfs.lazyfs
================

A class for lazy initialization of an FS object.

This module provides the class LazyFS, an FS wrapper class that can lazily
initialize its underlying FS object.

"""

import sys

try:
    from threading import Lock
except ImportError:
    from fs.base import DummyLock as Lock

from fs.base import FS
from fs.wrapfs import WrapFS


class LazyFS(WrapFS):
    """Simple 'lazy initialization' for FS objects.

    This FS wrapper can be created with an FS instance, an FS class, or a
    (class,args,kwds) tuple.  The actual FS instance will be created on demand
    the first time it is accessed.
    """

    def __init__(self, fs):
        super(LazyFS, self).__init__(fs)
        self._lazy_creation_lock = Lock()

    def __unicode__(self):
        try:
            wrapped_fs = self.__dict__["wrapped_fs"]
        except KeyError:
            #  It appears that python2.5 has trouble printing out
            #  classes that define a __unicode__ method.
            try:
                return u"<LazyFS: %s>" % (self._fsclass,)
            except TypeError:
                try:
                    return u"<LazyFS: %s>" % (self._fsclass.__name__,)
                except AttributeError:
                    return u"<LazyFS: <unprintable>>"
        else:
            return u"<LazyFS: %s>" % (wrapped_fs,)

    def __getstate__(self):
        state = super(LazyFS,self).__getstate__()
        del state["_lazy_creation_lock"]
        return state

    def __setstate__(self, state):
        super(LazyFS,self).__setstate__(state)
        self._lazy_creation_lock = Lock()

    def _get_wrapped_fs(self):
        """Obtain the wrapped FS instance, creating it if necessary."""
        try:
            fs = self.__dict__["wrapped_fs"]
        except KeyError:
            self._lazy_creation_lock.acquire()
            try:
                try:
                    fs = self.__dict__["wrapped_fs"]
                except KeyError:
                    fs = self._fsclass(*self._fsargs,**self._fskwds)
                    self.__dict__["wrapped_fs"] = fs
            finally:
                self._lazy_creation_lock.release()
        return fs

    def _set_wrapped_fs(self, fs):
        if isinstance(fs,FS):
            self.__dict__["wrapped_fs"] = fs
        elif isinstance(fs,type):
            self._fsclass = fs
            self._fsargs = []
            self._fskwds = {}
        elif fs is None:
            del self.__dict__['wrapped_fs']
        else:
            self._fsclass = fs[0]
            try:
                self._fsargs = fs[1]
            except IndexError:
                self._fsargs = []
            try:
                self._fskwds = fs[2]
            except IndexError:
                self._fskwds = {}

    wrapped_fs = property(_get_wrapped_fs,_set_wrapped_fs)

    def setcontents(self, path, data, chunk_size=64*1024):
        return self.wrapped_fs.setcontents(path, data, chunk_size=chunk_size)

    def close(self):
        if not self.closed:
            #  If it was never initialized, create a fake one to close.
            if "wrapped_fs" not in self.__dict__:
                self.__dict__["wrapped_fs"] = FS()
            super(LazyFS,self).close()


