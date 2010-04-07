"""
fs.osfs.watch_inotify
=============

Change watcher support for OSFS, backed by pyinotify.

"""

import os
import sys
import errno
import threading

from fs.errors import *
from fs.path import *
from fs.watch import *

import pyinotify


class OSFSWatchMixin(WatchableFSMixin):
    """Mixin providing change-watcher support via pyinotify."""

    __watch_lock = threading.Lock()
    __watch_manager = None
    __watch_notifier = None

    def close(self):
        super(OSFSWatchMixin,self).close()
        self.__shutdown_watch_manager(force=True)
        self.notify_watchers(CLOSED)

    def add_watcher(self,callback,path="/",events=None,recursive=True):
        w = super(OSFSWatchMixin,self).add_watcher(callback,path,events,recursive)
        syspath = self.getsyspath(path)
        if isinstance(syspath,unicode):
            syspath = syspath.encode(sys.getfilesystemencoding())
        wm = self.__get_watch_manager()
        evtmask = self.__get_event_mask(events)
        def process_events(event):
            self.__route_event(w,event)
        kwds = dict(rec=recursive,auto_add=recursive,quiet=False)
        try:
            wids = wm.add_watch(syspath,evtmask,process_events,**kwds)
        except pyinotify.WatchManagerError, e:
            raise OperationFailedError("add_watcher",details=e)
        w._pyinotify_id = wids[syspath]
        return w

    def del_watcher(self,watcher_or_callback):
        wm = self.__get_watch_manager()
        if isinstance(watcher_or_callback,Watcher):
            watchers = [watcher_or_callback]
        else:
            watchers = self._find_watchers(watcher_or_callback)
        for watcher in watchers:
            wm.rm_watch(watcher._pyinotify_id,rec=watcher.recursive)
            super(OSFSWatchMixin,self).del_watcher(watcher)
        if not wm._wmd:
            self.__shutdown_watch_manager()

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
            watcher.handle_event(MODIFIED(self,path,True))
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
            watcher.handle_event(CLOSE(self))
        
    def __get_watch_manager(self):
        """Get the shared watch manager, initializing if necessary."""
        if OSFSWatchMixin.__watch_notifier is None:
            self.__watch_lock.acquire()
            try:
                if self.__watch_notifier is None:
                    wm = pyinotify.WatchManager()
                    n = pyinotify.ThreadedNotifier(wm)
                    n.start()
                    OSFSWatchMixin.__watch_manager = wm
                    OSFSWatchMixin.__watch_notifier = n
            finally:
                self.__watch_lock.release()
        return OSFSWatchMixin.__watch_manager

    def __shutdown_watch_manager(self,force=False):
        """Stop the shared watch manager, if there are no watches left."""
        self.__watch_lock.acquire()
        try:
            if OSFSWatchMixin.__watch_manager is None:
                return
            if not force and OSFSWatchMixin.__watch_manager._wmd:
                return
            OSFSWatchMixin.__watch_notifier.stop()
            OSFSWatchMixin.__watch_notifier = None
            OSFSWatchMixin.__watch_manager = None
        finally:
            self.__watch_lock.release()


