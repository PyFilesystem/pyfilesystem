"""

  fs.tests.test_remote:  testcases for FS remote support utilities

"""

from fs.tests import FSTestCases, ThreadingTestCases

import unittest
import threading
import random
import time
import sys

from fs.remote import *

from fs.wrapfs import WrapFS, wrap_fs_methods
from fs.tempfs import TempFS
from fs.path import *
from fs.local_functools import wraps


class TestCacheFS(unittest.TestCase,FSTestCases,ThreadingTestCases):
    """Test simple operation of CacheFS"""

    def setUp(self):
        self._check_interval = sys.getcheckinterval()
        sys.setcheckinterval(10)
        self.fs = CacheFS(TempFS())

    def tearDown(self):
        self.fs.close()
        sys.setcheckinterval(self._check_interval)


class TestConnectionManagerFS(unittest.TestCase,FSTestCases,ThreadingTestCases):
    """Test simple operation of ConnectionManagerFS"""

    def setUp(self):
        self._check_interval = sys.getcheckinterval()
        sys.setcheckinterval(10)
        self.fs = ConnectionManagerFS(TempFS())

    def tearDown(self):
        self.fs.close()
        sys.setcheckinterval(self._check_interval)


class DisconnectingFS(WrapFS):
    """FS subclass that raises lots of RemoteConnectionErrors."""

    def __init__(self,fs=None):
        if fs is None:
            fs = TempFS()
        self._connected = True
        self._continue = True
        self._bounce_thread = None
        super(DisconnectingFS,self).__init__(fs)
        if random.choice([True,False]):
            raise RemoteConnectionError("")
        self._bounce_thread = threading.Thread(target=self._bounce)
        self._bounce_thread.start()

    def __getstate__(self):
        state = super(DisconnectingFS,self).__getstate__()
        del state["_bounce_thread"]
        return state

    def __setstate__(self,state):
        super(DisconnectingFS,self).__setstate__(state)
        self._bounce_thread = threading.Thread(target=self._bounce)
        self._bounce_thread.start()

    def _bounce(self):
        while self._continue:
            time.sleep(random.random()*0.1)
            self._connected = not self._connected

    def setcontents(self, path, contents):
        return self.wrapped_fs.setcontents(path, contents)

    def close(self):
        if not self.closed:
            self._continue = False
            if self._bounce_thread is not None:
                self._bounce_thread.join()
            self._connected = True
            super(DisconnectingFS,self).close()


def disconnecting_wrapper(func):
    """Method wrapper to raise RemoteConnectionError if not connected."""
    @wraps(func)
    def wrapper(self,*args,**kwds):
        if not self._connected:
            raise RemoteConnectionError("")
        return func(self,*args,**kwds)
    return wrapper
DisconnectingFS = wrap_fs_methods(disconnecting_wrapper,DisconnectingFS,exclude=["close"])


class DisconnectRecoveryFS(WrapFS):
    """FS subclass that recovers from RemoteConnectionErrors by waiting."""
    pass
def recovery_wrapper(func):
    """Method wrapper to recover from RemoteConnectionErrors by waiting."""
    @wraps(func)
    def wrapper(self,*args,**kwds):
        while True:
            try:
                return func(self,*args,**kwds)
            except RemoteConnectionError:
                self.wrapped_fs.wait_for_connection()
    return wrapper
# this also checks that wrap_fs_methods works as a class decorator
DisconnectRecoveryFS = wrap_fs_methods(recovery_wrapper)(DisconnectRecoveryFS)


class TestConnectionManagerFS_disconnect(TestConnectionManagerFS):
    """Test ConnectionManagerFS's ability to wait for reconnection."""

    def setUp(self):
        self._check_interval = sys.getcheckinterval()
        sys.setcheckinterval(10)
        c_fs = ConnectionManagerFS(DisconnectingFS,poll_interval=0.1)
        self.fs = DisconnectRecoveryFS(c_fs)

    def tearDown(self):
        self.fs.close()
        sys.setcheckinterval(self._check_interval)


