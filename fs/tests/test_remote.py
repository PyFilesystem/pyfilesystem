#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from fs import SEEK_END
from fs.wrapfs import WrapFS, wrap_fs_methods
from fs.tempfs import TempFS
from fs.path import *
from fs.local_functools import wraps

from six import PY3, b


class RemoteTempFS(TempFS):
    """
        Simple filesystem implementing setfilecontents
        for RemoteFileBuffer tests
    """
    def __repr__(self):
        return '<RemoteTempFS: %s>' % self._temp_dir

    def open(self, path, mode='rb', write_on_flush=True, **kwargs):
        if 'a' in mode or 'r' in mode or '+' in mode:
            f = super(RemoteTempFS, self).open(path, mode='rb', **kwargs)
            f = TellAfterCloseFile(f)
        else:
            f = None

        return RemoteFileBuffer(self,
                                path,
                                mode,
                                f,
                                write_on_flush=write_on_flush)

    def setcontents(self, path, data, encoding=None, errors=None, chunk_size=64*1024):
        f = super(RemoteTempFS, self).open(path, 'wb', encoding=encoding, errors=errors, chunk_size=chunk_size)
        if getattr(data, 'read', False):
            f.write(data.read())
        else:
            f.write(data)
        f.close()


class TellAfterCloseFile(object):
    """File-like object that allows calling tell() after it's been closed."""

    def __init__(self, file):
        self._finalpos = None
        self.file = file

    def close(self):
        if self._finalpos is None:
            self._finalpos = self.file.tell()
        self.file.close()

    def tell(self):
        if self._finalpos is not None:
            return self._finalpos
        return self.file.tell()

    def __getattr__(self, attr):
        return getattr(self.file, attr)


class TestRemoteFileBuffer(unittest.TestCase, FSTestCases, ThreadingTestCases):
    class FakeException(Exception): pass

    def setUp(self):
        self.fs = RemoteTempFS()
        self.original_setcontents = self.fs.setcontents

    def tearDown(self):
        self.fs.close()
        self.fakeOff()

    def fake_setcontents(self, path, content=b(''), chunk_size=16*1024):
        ''' Fake replacement for RemoteTempFS setcontents() '''
        raise self.FakeException("setcontents should not be called here!")

    def fakeOn(self):
        '''
            Turn on fake_setcontents(). When setcontents on RemoteTempFS
            is called, FakeException is raised and nothing is stored.
        '''
        self.fs.setcontents = self.fake_setcontents

    def fakeOff(self):
        ''' Switch off fake_setcontents(). '''
        self.fs.setcontents = self.original_setcontents

    def test_ondemand(self):
        '''
            Tests on-demand loading of remote content in RemoteFileBuffer
        '''
        contents = b("Tristatricettri stribrnych strikacek strikalo") + \
                   b("pres tristatricettri stribrnych strech.")
        f = self.fs.open('test.txt', 'wb')
        f.write(contents)
        f.close()

        # During following tests, no setcontents() should be called.
        self.fakeOn()

        f = self.fs.open('test.txt', 'rb')
        self.assertEquals(f.read(10), contents[:10])
        f.wrapped_file.seek(0, SEEK_END)
        self.assertEquals(f._rfile.tell(), 10)
        f.seek(20)
        self.assertEquals(f.tell(), 20)
        self.assertEquals(f._rfile.tell(), 20)
        f.seek(0, SEEK_END)
        self.assertEquals(f._rfile.tell(), len(contents))
        f.close()

        f = self.fs.open('test.txt', 'ab')
        self.assertEquals(f.tell(), len(contents))
        f.close()

        self.fakeOff()

        # Writing over the rfile edge
        f = self.fs.open('test.txt', 'wb+')
        self.assertEquals(f.tell(), 0)
        f.seek(len(contents) - 5)
        # Last 5 characters not loaded from remote file
        self.assertEquals(f._rfile.tell(), len(contents) - 5)
        # Confirm that last 5 characters are still in rfile buffer
        self.assertEquals(f._rfile.read(), contents[-5:])
        # Rollback position 5 characters before eof
        f._rfile.seek(len(contents[:-5]))
        # Write 10 new characters (will make contents longer for 5 chars)
        f.write(b('1234567890'))
        f.flush()
        # We are on the end of file (and buffer not serve anything anymore)
        self.assertEquals(f.read(), b(''))
        f.close()

        self.fakeOn()

        # Check if we wrote everything OK from
        # previous writing over the remote buffer edge
        f = self.fs.open('test.txt', 'rb')
        self.assertEquals(f.read(), contents[:-5] + b('1234567890'))
        f.close()

        self.fakeOff()

    def test_writeonflush(self):
        '''
            Test 'write_on_flush' switch of RemoteFileBuffer.
            When True, flush() should call setcontents and store
            to remote destination.
            When False, setcontents should be called only on close().
        '''
        self.fakeOn()
        f = self.fs.open('test.txt', 'wb', write_on_flush=True)
        f.write(b('Sample text'))
        self.assertRaises(self.FakeException, f.flush)
        f.write(b('Second sample text'))
        self.assertRaises(self.FakeException, f.close)
        self.fakeOff()
        f.close()
        self.fakeOn()

        f = self.fs.open('test.txt', 'wb', write_on_flush=False)
        f.write(b('Sample text'))
        # FakeException is not raised, because setcontents is not called
        f.flush()
        f.write(b('Second sample text'))
        self.assertRaises(self.FakeException, f.close)
        self.fakeOff()

    def test_flush_and_continue(self):
        '''
            This tests if partially loaded remote buffer can be flushed
            back to remote destination and opened file is still
            in good condition.
        '''
        contents = b("Zlutoucky kun upel dabelske ody.")
        contents2 = b('Ententyky dva spaliky cert vyletel z elektriky')

        f = self.fs.open('test.txt', 'wb')
        f.write(contents)
        f.close()

        f = self.fs.open('test.txt', 'rb+')
        # Check if we read just 10 characters
        self.assertEquals(f.read(10), contents[:10])
        self.assertEquals(f._rfile.tell(), 10)
        # Write garbage to file to mark it as _changed
        f.write(b('x'))
        # This should read the rest of file and store file back to again.
        f.flush()
        f.seek(0)
        # Try if we have unocrrupted file locally...
        self.assertEquals(f.read(), contents[:10] + b('x') + contents[11:])
        f.close()

        # And if we have uncorrupted file also on storage
        f = self.fs.open('test.txt', 'rb')
        self.assertEquals(f.read(), contents[:10] + b('x') + contents[11:])
        f.close()

        # Now try it again, but write garbage behind edge of remote file
        f = self.fs.open('test.txt', 'rb+')
        self.assertEquals(f.read(10), contents[:10])
        # Write garbage to file to mark it as _changed
        f.write(contents2)
        f.flush()
        f.seek(0)
        # Try if we have unocrrupted file locally...
        self.assertEquals(f.read(), contents[:10] + contents2)
        f.close()

        # And if we have uncorrupted file also on storage
        f = self.fs.open('test.txt', 'rb')
        self.assertEquals(f.read(), contents[:10] + contents2)
        f.close()


class TestCacheFS(unittest.TestCase,FSTestCases,ThreadingTestCases):
    """Test simple operation of CacheFS"""

    def setUp(self):
        self._check_interval = sys.getcheckinterval()
        sys.setcheckinterval(10)
        self.wrapped_fs = TempFS()
        self.fs = CacheFS(self.wrapped_fs,cache_timeout=0.01)

    def tearDown(self):
        self.fs.close()
        sys.setcheckinterval(self._check_interval)

    def test_values_are_used_from_cache(self):
        old_timeout = self.fs.cache_timeout
        self.fs.cache_timeout = None
        try:
            self.assertFalse(self.fs.isfile("hello"))
            self.wrapped_fs.setcontents("hello",b("world"))
            self.assertTrue(self.fs.isfile("hello"))
            self.wrapped_fs.remove("hello")
            self.assertTrue(self.fs.isfile("hello"))
            self.fs.clear_cache()
            self.assertFalse(self.fs.isfile("hello"))
        finally:
            self.fs.cache_timeout = old_timeout

    def test_values_are_updated_in_cache(self):
        old_timeout = self.fs.cache_timeout
        self.fs.cache_timeout = None
        try:
            self.assertFalse(self.fs.isfile("hello"))
            self.wrapped_fs.setcontents("hello",b("world"))
            self.assertTrue(self.fs.isfile("hello"))
            self.wrapped_fs.remove("hello")
            self.assertTrue(self.fs.isfile("hello"))
            self.wrapped_fs.setcontents("hello",b("world"))
            self.assertTrue(self.fs.isfile("hello"))
            self.fs.remove("hello")
            self.assertFalse(self.fs.isfile("hello"))
        finally:
            self.fs.cache_timeout = old_timeout



class TestConnectionManagerFS(unittest.TestCase,FSTestCases):#,ThreadingTestCases):
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
        self._bounce_thread.daemon = True
        self._bounce_thread.start()

    def __getstate__(self):
        state = super(DisconnectingFS,self).__getstate__()
        del state["_bounce_thread"]
        return state

    def __setstate__(self,state):
        super(DisconnectingFS,self).__setstate__(state)
        self._bounce_thread = threading.Thread(target=self._bounce)
        self._bounce_thread.daemon = True
        self._bounce_thread.start()

    def _bounce(self):
        while self._continue:
            time.sleep(random.random()*0.1)
            self._connected = not self._connected

    def setcontents(self, path, data=b(''), encoding=None, errors=None, chunk_size=64*1024):
        return self.wrapped_fs.setcontents(path, data, encoding=encoding, errors=errors, chunk_size=chunk_size)

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

if __name__ == '__main__':
    unittest.main()
