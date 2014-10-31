"""
fs.osfs.watch_inotify
=============

Change watcher support for OSFS, backed by pyinotify.

"""

import os
import sys
import errno
import select
import threading

from fs.errors import *
from fs.path import *
from fs.watch import *

try:
    import pyinotify
except Exception, e:
    #  pyinotify sometimes raises its own custom errors on import.
    #  How on earth are we supposed to catch them when we can't import them?
    if isinstance(e,ImportError):
        raise
    raise ImportError("could not import pyinotify")
try:
    pyinotify.WatchManager.get_fd
except AttributeError:
    raise ImportError("pyinotify version is too old")


class OSFSWatchMixin(WatchableFSMixin):
    """Mixin providing change-watcher support via pyinotify."""

    __watch_lock = threading.Lock()
    __watch_thread = None

    def close(self):
        super(OSFSWatchMixin,self).close()
        self.notify_watchers(CLOSED)
        for watcher_list in self._watchers.values():
            for watcher in watcher_list:
                self.del_watcher(watcher)
        self.__watch_lock.acquire()
        try:
            wt = self.__watch_thread
            if wt is not None and not wt.watchers:
                wt.stop()
                wt.join()
                OSFSWatchMixin.__watch_thread = None
        finally:
            self.__watch_lock.release()

    @convert_os_errors
    def add_watcher(self,callback,path="/",events=None,recursive=True):
        super_add_watcher = super(OSFSWatchMixin,self).add_watcher
        w = super_add_watcher(callback,path,events,recursive)
        w._pyinotify_id = None
        syspath = self.getsyspath(path)
        if isinstance(syspath,unicode):
            syspath = syspath.encode(sys.getfilesystemencoding())
        #  Each watch gets its own WatchManager, since it's tricky to make
        #  a single WatchManager handle multiple callbacks with different
        #  events for a single path.  This means we pay one file descriptor
        #  for each watcher added to the filesystem.  That's not too bad.
        w._pyinotify_WatchManager = wm = pyinotify.WatchManager()
        #  Each individual notifier gets multiplexed by a single shared thread.
        w._pyinotify_Notifier = pyinotify.Notifier(wm)
        evtmask = self.__get_event_mask(events)
        def process_events(event):
            self.__route_event(w,event)
        kwds = dict(rec=recursive,auto_add=recursive,quiet=False)
        try:
            wids = wm.add_watch(syspath,evtmask,process_events,**kwds)
        except pyinotify.WatchManagerError, e:
            raise OperationFailedError("add_watcher",details=e)
        w._pyinotify_id = wids[syspath]
        self.__watch_lock.acquire()
        try:
            wt = self.__get_watch_thread()
            wt.add_watcher(w)
        finally:
            self.__watch_lock.release()
        return w

    @convert_os_errors
    def del_watcher(self,watcher_or_callback):
        if isinstance(watcher_or_callback,Watcher):
            watchers = [watcher_or_callback]
        else:
            watchers = self._find_watchers(watcher_or_callback)
        for watcher in watchers:
            wm = watcher._pyinotify_WatchManager
            wm.rm_watch(watcher._pyinotify_id,rec=watcher.recursive)
            super(OSFSWatchMixin,self).del_watcher(watcher)
        self.__watch_lock.acquire()
        try:
            wt = self.__get_watch_thread()
            for watcher in watchers:
                wt.del_watcher(watcher)
        finally:
            self.__watch_lock.release()

    def __get_event_mask(self,events):
        """Convert the given set of events into a pyinotify event mask."""
        if events is None:
            events = (EVENT,)
        mask = 0
        for evt in events:
            if issubclass(ACCESSED,evt):
                mask |= pyinotify.IN_ACCESS
            if issubclass(CREATED,evt):
                mask |= pyinotify.IN_CREATE
            if issubclass(REMOVED,evt):
                mask |= pyinotify.IN_DELETE
                mask |= pyinotify.IN_DELETE_SELF
            if issubclass(MODIFIED,evt):
                mask |= pyinotify.IN_ATTRIB
                mask |= pyinotify.IN_MODIFY
                mask |= pyinotify.IN_CLOSE_WRITE
            if issubclass(MOVED_SRC,evt):
                mask |= pyinotify.IN_MOVED_FROM
                mask |= pyinotify.IN_MOVED_TO
            if issubclass(MOVED_DST,evt):
                mask |= pyinotify.IN_MOVED_FROM
                mask |= pyinotify.IN_MOVED_TO
            if issubclass(OVERFLOW,evt):
                mask |= pyinotify.IN_Q_OVERFLOW
            if issubclass(CLOSED,evt):
                mask |= pyinotify.IN_UNMOUNT
        return mask

    def __route_event(self,watcher,inevt):
        """Convert pyinotify event into fs.watch event, then handle it."""
        try:
            path = self.unsyspath(inevt.pathname)
        except ValueError:
            return
        try:
            src_path = inevt.src_pathname
            if src_path is not None:
                src_path = self.unsyspath(src_path)
        except (AttributeError,ValueError):
            src_path = None
        if inevt.mask & pyinotify.IN_ACCESS:
            watcher.handle_event(ACCESSED(self,path))
        if inevt.mask & pyinotify.IN_CREATE:
            watcher.handle_event(CREATED(self,path))
            #  Recursive watching of directories in pyinotify requires
            #  the creation of a new watch for each subdir, resulting in
            #  a race condition whereby events in the subdir are missed.
            #  We'd prefer to duplicate events than to miss them.
            if inevt.mask & pyinotify.IN_ISDIR:
                try:
                    #  pyinotify does this for dirs itself, we only.
                    #  need to worry about newly-created files.
                    for child in self.listdir(path,files_only=True):
                        cpath = pathjoin(path,child)
                        self.notify_watchers(CREATED,cpath)
                        self.notify_watchers(MODIFIED,cpath,True)
                except FSError:
                    pass
        if inevt.mask & pyinotify.IN_DELETE:
            watcher.handle_event(REMOVED(self,path))
        if inevt.mask & pyinotify.IN_DELETE_SELF:
            watcher.handle_event(REMOVED(self,path))
        if inevt.mask & pyinotify.IN_ATTRIB:
            watcher.handle_event(MODIFIED(self,path,False))
        if inevt.mask & pyinotify.IN_MODIFY:
            watcher.handle_event(MODIFIED(self,path,True))
        if inevt.mask & pyinotify.IN_CLOSE_WRITE:
            watcher.handle_event(MODIFIED(self,path,True, closed=True))
        if inevt.mask & pyinotify.IN_MOVED_FROM:
            # Sorry folks, I'm not up for decoding the destination path.
            watcher.handle_event(MOVED_SRC(self,path,None))
        if inevt.mask & pyinotify.IN_MOVED_TO:
            if getattr(inevt,"src_pathname",None):
                watcher.handle_event(MOVED_SRC(self,src_path,path))
                watcher.handle_event(MOVED_DST(self,path,src_path))
            else:
                watcher.handle_event(MOVED_DST(self,path,None))
        if inevt.mask & pyinotify.IN_Q_OVERFLOW:
            watcher.handle_event(OVERFLOW(self))
        if inevt.mask & pyinotify.IN_UNMOUNT:
            watcher.handle_event(CLOSED(self))

    def __get_watch_thread(self):
        """Get the shared watch thread, initializing if necessary.

        This method must only be called while holding self.__watch_lock, or
        multiple notifiers could be created.
        """
        if OSFSWatchMixin.__watch_thread is None:
            OSFSWatchMixin.__watch_thread = SharedThreadedNotifier()
            OSFSWatchMixin.__watch_thread.start()
        return OSFSWatchMixin.__watch_thread


class SharedThreadedNotifier(threading.Thread):
    """pyinotifer Notifier that can manage multiple WatchManagers.

    Each watcher added to an OSFS corresponds to a new pyinotify.WatchManager
    instance.  Rather than run a notifier thread for each manager, we run a
    single thread that multiplexes between them all.
    """

    def __init__(self):
        super(SharedThreadedNotifier,self).__init__()
        self.daemon = True
        self.running = True
        self._pipe_r, self._pipe_w = os.pipe()
        self._poller = select.poll()
        self._poller.register(self._pipe_r,select.POLLIN)
        self.watchers = {}

    def add_watcher(self,watcher):
        fd = watcher._pyinotify_WatchManager.get_fd()
        self.watchers[fd] = watcher
        self._poller.register(fd,select.POLLIN)
        #  Bump the poll object so it recognises the new fd.
        os.write(self._pipe_w,b"H")

    def del_watcher(self,watcher):
        fd = watcher._pyinotify_WatchManager.get_fd()
        try:
            del self.watchers[fd]
        except KeyError:
            pass
        else:
            self._poller.unregister(fd)

    def run(self):
        #  Grab some attributes of the select module, so they're available
        #  even when shutting down the interpreter.
        _select_error = select.error
        _select_POLLIN = select.POLLIN
        #  Loop until stopped, dispatching to individual notifiers.
        while self.running:
            try:
                ready_fds = self._poller.poll()
            except _select_error, e:
                if e[0] != errno.EINTR:
                    raise
            else:
                for (fd,event) in ready_fds:
                    #  Ignore all events other than "input ready".
                    if not event & _select_POLLIN:
                        continue
                    #  For signals on our internal pipe, just read and discard.
                    if fd == self._pipe_r:
                        os.read(self._pipe_r,1)
                    #  For notifier fds, dispath to the notifier methods.
                    else:
                        try:
                            notifier = self.watchers[fd]._pyinotify_Notifier
                        except KeyError:
                            pass
                        else:
                            notifier.read_events()
                            try:
                                notifier.process_events()
                            except EnvironmentError:
                                pass

    def stop(self):
        if self.running:
            self.running = False
            os.write(self._pipe_w,"S")
            os.close(self._pipe_w)


