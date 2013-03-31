"""

  fs.tests.test_watch:  testcases for change watcher support

"""

import os
import sys
import time
import gc
import pickle
import unittest

from fs.path import *
from fs.errors import *
from fs.watch import *
from fs.tests import FSTestCases

try:
    from fs.osfs import watch_inotify
except ImportError:
    watch_inotify = None

if sys.platform == "win32":
    try:
        from fs.osfs import watch_win32
    except ImportError:
        watch_win32 = None
else:
    watch_win32 = None

import logging
logging.getLogger('pyinotify').setLevel(logging.ERROR)


import six
from six import PY3, b

class WatcherTestCases:
    """Testcases for filesystems providing change watcher support.

    This class should be used as a mixin to the unittest.TestCase class
    for filesystems that provide change watcher support.
    """

    def setupWatchers(self):
        self._captured_events = []
        self.watchfs.add_watcher(self._captured_events.append)

    def clearCapturedEvents(self):
        del self._captured_events[:]

    def waitForEvents(self):
        if isinstance(self.watchfs,PollingWatchableFS):
            self.watchfs._poll_cond.acquire()
            self.watchfs._poll_cond.wait()
            self.watchfs._poll_cond.wait()
            self.watchfs._poll_cond.release()
        else:
            time.sleep(2)

    def assertEventOccurred(self,cls,path=None,event_list=None,**attrs):
        if not self.checkEventOccurred(cls,path,event_list,**attrs):
            args = (cls.__name__,path,attrs)
            assert False, "Event did not occur: %s(%s,%s)" % args

    def checkEventOccurred(self,cls,path=None,event_list=None,**attrs):
        if event_list is None:
            event_list = self._captured_events
        self.waitForEvents()
        for event in event_list:
            if isinstance(event,cls):
                if path is None or event.path == path:
                    for (k,v) in attrs.iteritems():
                        if getattr(event,k) != v:
                            break
                    else:
                        # all attrs match - found it!
                        return True
        return False

    def test_watch_makedir(self):
        self.setupWatchers()
        self.fs.makedir("test1")
        self.assertEventOccurred(CREATED,"/test1")

    def test_watch_makedir_with_two_watchers(self):
        self.setupWatchers()
        events2 = []
        self.watchfs.add_watcher(events2.append)
        self.fs.makedir("test1")
        self.assertEventOccurred(CREATED,"/test1")
        self.assertEventOccurred(CREATED,"/test1",event_list=events2)

    def test_watch_readfile(self):
        self.setupWatchers()
        self.fs.setcontents("hello", b("hello world"))
        self.assertEventOccurred(CREATED,"/hello")
        self.clearCapturedEvents()
        old_atime = self.fs.getinfo("hello").get("accessed_time")
        self.assertEquals(self.fs.getcontents("hello"), b("hello world"))
        if not isinstance(self.watchfs,PollingWatchableFS):
            #  Help it along by updting the atime.
            #  TODO: why is this necessary?
            if self.fs.hassyspath("hello"):
                syspath = self.fs.getsyspath("hello")
                mtime = os.stat(syspath).st_mtime
                atime = int(time.time())
                os.utime(self.fs.getsyspath("hello"),(atime,mtime))
            self.assertEventOccurred(ACCESSED,"/hello")
        elif old_atime is not None:
            #  Some filesystems don't update atime synchronously, or only
            #  update it if it's too old, or don't update it at all!
            #  Try to force the issue, wait for it to change, but eventually
            #  give up and bail out.
            for i in xrange(10):
                if self.fs.getinfo("hello").get("accessed_time") != old_atime:
                    if not self.checkEventOccurred(MODIFIED,"/hello"):
                        self.assertEventOccurred(ACCESSED,"/hello")
                    break
                time.sleep(0.2)
                if self.fs.hassyspath("hello"):
                    syspath = self.fs.getsyspath("hello")
                    mtime = os.stat(syspath).st_mtime
                    atime = int(time.time())
                    os.utime(self.fs.getsyspath("hello"),(atime,mtime))

    def test_watch_writefile(self):
        self.setupWatchers()
        self.fs.setcontents("hello", b("hello world"))
        self.assertEventOccurred(CREATED,"/hello")
        self.clearCapturedEvents()
        self.fs.setcontents("hello", b("hello again world"))
        self.assertEventOccurred(MODIFIED,"/hello")

    def test_watch_single_file(self):
        self.fs.setcontents("hello", b("hello world"))
        events = []
        self.watchfs.add_watcher(events.append,"/hello",(MODIFIED,))
        self.fs.setcontents("hello", b("hello again world"))
        self.fs.remove("hello")
        self.waitForEvents()
        for evt in events:
            assert isinstance(evt,MODIFIED)
            self.assertEquals(evt.path,"/hello")

    def test_watch_single_file_remove(self):
        self.fs.makedir("testing")
        self.fs.setcontents("testing/hello", b("hello world"))
        events = []
        self.watchfs.add_watcher(events.append,"/testing/hello",(REMOVED,))
        self.fs.setcontents("testing/hello", b("hello again world"))
        self.waitForEvents()
        self.fs.remove("testing/hello")
        self.waitForEvents()
        self.assertEquals(len(events),1)
        assert isinstance(events[0],REMOVED)
        self.assertEquals(events[0].path,"/testing/hello")

    def test_watch_iter_changes(self):
        changes = iter_changes(self.watchfs)
        self.fs.makedir("test1")
        self.fs.setcontents("test1/hello", b("hello world"))
        self.waitForEvents()
        self.fs.removedir("test1",force=True)
        self.waitForEvents()
        self.watchfs.close()
        #  Locate the CREATED(test1) event
        event = changes.next(timeout=1)
        while not isinstance(event,CREATED) or event.path != "/test1":
            event = changes.next(timeout=1)
        #  Locate the CREATED(test1/hello) event
        event = changes.next(timeout=1)
        while not isinstance(event,CREATED) or event.path != "/test1/hello":
            event = changes.next(timeout=1)
        #  Locate the REMOVED(test1) event
        event = changes.next(timeout=1)
        while not isinstance(event,REMOVED) or event.path != "/test1":
            event = changes.next(timeout=1)
        #  Locate the CLOSED event
        event = changes.next(timeout=1)
        while not isinstance(event,CLOSED):
            event = changes.next(timeout=1)
        #  That should be the last event in the list
        self.assertRaises(StopIteration,getattr(changes, "next"),timeout=1)
        changes.close()



from fs import tempfs, osfs
class TestWatchers_TempFS(unittest.TestCase,FSTestCases,WatcherTestCases):

    def setUp(self):
        self.fs = tempfs.TempFS()
        watchfs = osfs.OSFS(self.fs.root_path)
        self.watchfs = ensure_watchable(watchfs,poll_interval=0.1)
        if watch_inotify is not None:
            self.assertEquals(watchfs,self.watchfs)
        if watch_win32 is not None:
            self.assertEquals(watchfs,self.watchfs)

    def tearDown(self):
        self.watchfs.close()
        self.fs.close()

    def check(self, p):
        return self.fs.exists(p)


from fs import memoryfs
class TestWatchers_MemoryFS(unittest.TestCase,FSTestCases,WatcherTestCases):

    def setUp(self):
        self.fs = self.watchfs = WatchableFS(memoryfs.MemoryFS())

    def tearDown(self):
        self.watchfs.close()
        self.fs.close()

    def check(self, p):
        return self.fs.exists(p)


class TestWatchers_MemoryFS_polling(TestWatchers_MemoryFS):

    def setUp(self):
        self.fs = memoryfs.MemoryFS()
        self.watchfs = ensure_watchable(self.fs,poll_interval=0.1)
