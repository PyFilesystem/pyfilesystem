"""
fs.watch
========

Change notification support for FS.

This module defines a standard interface for FS subclasses that support change
notification callbacks.  It also offers some WrapFS subclasses that can
simulate such an ability on top of an ordinary FS object.

An FS object that wants to be "watchable" must provide the following methods:

  * ``add_watcher(callback,path="/",events=None,recursive=True)``

      Request that the given callback be executed in response to changes
      to the given path.  A specific set of change events can be specified.
      This method returns a Watcher object.

  * ``del_watcher(watcher_or_callback)``

      Remove the given watcher object, or any watchers associated with
      the given callback.


If you would prefer to read changes from a filesystem in a blocking fashion
rather than using callbacks, you can use the function 'iter_changes' to obtain
an iterator over the change events.


"""

import sys
import weakref
import threading
import Queue
import traceback

from fs.path import *
from fs.errors import *
from fs.wrapfs import WrapFS
from fs.base import FS
from fs.filelike import FileWrapper

from six import b


class EVENT(object):
    """Base class for change notification events."""
    def __init__(self,fs,path):
        super(EVENT, self).__init__()
        self.fs = fs
        if path is not None:
            path = abspath(normpath(path))
        self.path = path

    def __str__(self):
        return unicode(self).encode("utf8")

    def __unicode__(self):
        return u"<fs.watch.%s object (path='%s') at %s>" % (self.__class__.__name__,self.path,hex(id(self)))

    def clone(self,fs=None,path=None):
        if fs is None:
            fs = self.fs
        if path is None:
            path = self.path
        return self.__class__(fs,path)


class ACCESSED(EVENT):
    """Event fired when a file's contents are accessed."""
    pass

class CREATED(EVENT):
    """Event fired when a new file or directory is created."""
    pass

class REMOVED(EVENT):
    """Event fired when a file or directory is removed."""
    pass

class MODIFIED(EVENT):
    """Event fired when a file or directory is modified."""
    def __init__(self,fs,path,data_changed=False, closed=False):
        super(MODIFIED,self).__init__(fs,path)
        self.data_changed = data_changed
        self.closed = closed

    def clone(self,fs=None,path=None,data_changed=None):
        evt = super(MODIFIED,self).clone(fs,path)
        if data_changed is None:
            data_changed = self.data_changed
        evt.data_changed = data_changed
        return evt

class MOVED_DST(EVENT):
    """Event fired when a file or directory is the target of a move."""
    def __init__(self,fs,path,source=None):
        super(MOVED_DST,self).__init__(fs,path)
        if source is not None:
            source = abspath(normpath(source))
        self.source = source

    def __unicode__(self):
        return u"<fs.watch.%s object (path=%r,src=%r) at %s>" % (self.__class__.__name__,self.path,self.source,hex(id(self)))

    def clone(self,fs=None,path=None,source=None):
        evt = super(MOVED_DST,self).clone(fs,path)
        if source is None:
            source = self.source
        evt.source = source
        return evt

class MOVED_SRC(EVENT):
    """Event fired when a file or directory is the source of a move."""
    def __init__(self,fs,path,destination=None):
        super(MOVED_SRC,self).__init__(fs,path)
        if destination is not None:
            destination = abspath(normpath(destination))
        self.destination = destination

    def __unicode__(self):
        return u"<fs.watch.%s object (path=%r,dst=%r) at %s>" % (self.__class__.__name__,self.path,self.destination,hex(id(self)))

    def clone(self,fs=None,path=None,destination=None):
        evt = super(MOVED_SRC,self).clone(fs,path)
        if destination is None:
            destination = self.destination
        evt.destination = destination
        return evt

class CLOSED(EVENT):
    """Event fired when the filesystem is closed."""
    pass

class ERROR(EVENT):
    """Event fired when some miscellaneous error occurs."""
    pass

class OVERFLOW(ERROR):
    """Event fired when some events could not be processed."""
    pass



class Watcher(object):
    """Object encapsulating filesystem watch info."""

    def __init__(self,fs,callback,path="/",events=None,recursive=True):
        if events is None:
            events = (EVENT,)
        else:
            events = tuple(events)
        # Since the FS probably holds a reference to the Watcher, keeping
        # a reference back to the FS would create a cycle containing a
        # __del__ method.  Use a weakref to avoid this.
        self._w_fs = weakref.ref(fs)
        self.callback = callback
        self.path = abspath(normpath(path))
        self.events = events
        self.recursive = recursive

    @property
    def fs(self):
        return self._w_fs()

    def delete(self):
        fs = self.fs
        if fs is not None:
            fs.del_watcher(self)

    def handle_event(self,event):
        if not isinstance(event,self.events):
            return
        if event.path is not None:
            if not isprefix(self.path,event.path):
                return
            if not self.recursive:
                if event.path != self.path:
                    if dirname(event.path) != self.path:
                        return
        try:
            self.callback(event)
        except Exception:
            print >>sys.stderr, "error in FS watcher callback", self.callback
            traceback.print_exc()


class WatchableFSMixin(FS):
    """Mixin class providing watcher management functions."""

    def __init__(self,*args,**kwds):
        self._watchers = PathMap()
        super(WatchableFSMixin,self).__init__(*args,**kwds)

    def __getstate__(self):
        state = super(WatchableFSMixin,self).__getstate__()
        state.pop("_watchers",None)
        return state

    def __setstate__(self,state):
        super(WatchableFSMixin,self).__setstate__(state)
        self._watchers = PathMap()

    def add_watcher(self,callback,path="/",events=None,recursive=True):
        """Add a watcher callback to the FS."""
        w = Watcher(self,callback,path,events,recursive=recursive)
        self._watchers.setdefault(path,[]).append(w)
        return w

    def del_watcher(self,watcher_or_callback):
        """Delete a watcher callback from the FS."""
        if isinstance(watcher_or_callback,Watcher):
            self._watchers[watcher_or_callback.path].remove(watcher_or_callback)
        else:
            for watchers in self._watchers.itervalues():
                for i,watcher in enumerate(watchers):
                    if watcher.callback is watcher_or_callback:
                        del watchers[i]
                        break

    def _find_watchers(self,callback):
        """Find watchers registered with the given callback."""
        for watchers in self._watchers.itervalues():
            for watcher in watchers:
                if watcher.callback is callback:
                    yield watcher

    def notify_watchers(self,event_or_class,path=None,*args,**kwds):
        """Notify watchers of the given event data."""
        if isinstance(event_or_class,EVENT):
            event = event_or_class
        else:
            event = event_or_class(self,path,*args,**kwds)
        if path is None:
            path = event.path
        if path is None:
            for watchers in self._watchers.itervalues():
                for watcher in watchers:
                    watcher.handle_event(event)
        else:
            for prefix in recursepath(path):
                if prefix in self._watchers:
                    for watcher in self._watchers[prefix]:
                        watcher.handle_event(event)



class WatchedFile(FileWrapper):
    """File wrapper for use with WatchableFS.

    This file wrapper provides access to a file opened from a WatchableFS
    instance, and fires MODIFIED events when the file is modified.
    """

    def __init__(self,file,fs,path,mode=None):
        super(WatchedFile,self).__init__(file,mode)
        self.fs = fs
        self.path = path
        self.was_modified = False

    def _write(self,string,flushing=False):
        self.was_modified = True
        return super(WatchedFile,self)._write(string,flushing=flushing)

    def _truncate(self,size):
        self.was_modified = True
        return super(WatchedFile,self)._truncate(size)

    def flush(self):
        super(WatchedFile,self).flush()
        #  Don't bother if python if being torn down
        if Watcher is not None:
            if self.was_modified:
                self.fs.notify_watchers(MODIFIED,self.path,True)

    def close(self):
        super(WatchedFile,self).close()
        #  Don't bother if python if being torn down
        if Watcher is not None:
            if self.was_modified:
                self.fs.notify_watchers(MODIFIED,self.path,True)


class WatchableFS(WatchableFSMixin,WrapFS):
    """FS wrapper simulating watcher callbacks.

    This FS wrapper intercepts method calls that modify the underlying FS
    and generates appropriate notification events.  It thus allows watchers
    to monitor changes made through the underlying FS object, but not changes
    that might be made through other interfaces to the same filesystem.
    """

    def __init__(self, *args, **kwds):
        super(WatchableFS, self).__init__(*args, **kwds)

    def close(self):
        super(WatchableFS, self).close()
        self.notify_watchers(CLOSED)

    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        existed = self.wrapped_fs.isfile(path)
        f = super(WatchableFS, self).open(path,
                                          mode=mode,
                                          buffering=buffering,
                                          encoding=encoding,
                                          errors=errors,
                                          newline=newline,
                                          line_buffering=line_buffering,
                                          **kwargs)
        if not existed:
            self.notify_watchers(CREATED, path)
        self.notify_watchers(ACCESSED, path)
        return WatchedFile(f, self, path, mode)

    def setcontents(self, path, data=b'', encoding=None, errors=None, chunk_size=64*1024):
        existed = self.wrapped_fs.isfile(path)
        ret = super(WatchableFS, self).setcontents(path, data=data, encoding=encoding, errors=errors, chunk_size=chunk_size)
        if not existed:
            self.notify_watchers(CREATED, path)
        self.notify_watchers(ACCESSED, path)
        if data:
            self.notify_watchers(MODIFIED, path, True)
        return ret

    def createfile(self, path, wipe=False):
        existed = self.wrapped_fs.isfile(path)
        ret = super(WatchableFS, self).createfile(path, wipe=wipe)
        if not existed:
            self.notify_watchers(CREATED,path)
        self.notify_watchers(ACCESSED,path)
        return ret

    def makedir(self,path,recursive=False,allow_recreate=False):
        existed = self.wrapped_fs.isdir(path)
        try:
            super(WatchableFS,self).makedir(path,allow_recreate=allow_recreate)
        except ParentDirectoryMissingError:
            if not recursive:
                raise
            parent = dirname(path)
            if parent != path:
                self.makedir(dirname(path),recursive=True,allow_recreate=True)
            super(WatchableFS,self).makedir(path,allow_recreate=allow_recreate)
        if not existed:
            self.notify_watchers(CREATED,path)

    def remove(self,path):
        super(WatchableFS,self).remove(path)
        self.notify_watchers(REMOVED,path)

    def removedir(self,path,recursive=False,force=False):
        if not force:
            for nm in self.listdir(path):
                raise DirectoryNotEmptyError(path)
        else:
            for nm in self.listdir(path,dirs_only=True):
                try:
                    self.removedir(pathjoin(path,nm),force=True)
                except ResourceNotFoundError:
                    pass
            for nm in self.listdir(path,files_only=True):
                try:
                    self.remove(pathjoin(path,nm))
                except ResourceNotFoundError:
                    pass
        super(WatchableFS,self).removedir(path)
        self.notify_watchers(REMOVED,path)
        if recursive:
            parent = dirname(path)
            while parent and not self.listdir(parent):
                super(WatchableFS,self).removedir(parent)
                self.notify_watchers(REMOVED,parent)
                parent = dirname(parent)

    def rename(self,src,dst):
        d_existed = self.wrapped_fs.exists(dst)
        super(WatchableFS,self).rename(src,dst)
        if d_existed:
            self.notify_watchers(REMOVED,dst)
        self.notify_watchers(MOVED_DST,dst,src)
        self.notify_watchers(MOVED_SRC,src,dst)

    def copy(self,src,dst,**kwds):
        d = self._pre_copy(src,dst)
        super(WatchableFS,self).copy(src,dst,**kwds)
        self._post_copy(src,dst,d)

    def copydir(self,src,dst,**kwds):
        d = self._pre_copy(src,dst)
        super(WatchableFS,self).copydir(src,dst,**kwds)
        self._post_copy(src,dst,d)

    def move(self,src,dst,**kwds):
        d = self._pre_copy(src,dst)
        super(WatchableFS,self).move(src,dst,**kwds)
        self._post_copy(src,dst,d)
        self._post_move(src,dst,d)

    def movedir(self,src,dst,**kwds):
        d = self._pre_copy(src,dst)
        super(WatchableFS,self).movedir(src,dst,**kwds)
        self._post_copy(src,dst,d)
        self._post_move(src,dst,d)

    def _pre_copy(self,src,dst):
        dst_paths = {}
        try:
            for (dirnm,filenms) in self.wrapped_fs.walk(dst):
                dirnm = dirnm[len(dst)+1:]
                dst_paths[dirnm] = True
                for filenm in filenms:
                    dst_paths[filenm] = False
        except ResourceNotFoundError:
            pass
        except ResourceInvalidError:
            dst_paths[""] = False
        src_paths = {}
        try:
            for (dirnm,filenms) in self.wrapped_fs.walk(src):
                dirnm = dirnm[len(src)+1:]
                src_paths[dirnm] = True
                for filenm in filenms:
                    src_paths[pathjoin(dirnm,filenm)] = False
        except ResourceNotFoundError:
            pass
        except ResourceInvalidError:
            src_paths[""] = False
        return (src_paths,dst_paths)

    def _post_copy(self,src,dst,data):
        (src_paths,dst_paths) = data
        for src_path,isdir in sorted(src_paths.items()):
            path = pathjoin(dst,src_path)
            if src_path in dst_paths:
                self.notify_watchers(MODIFIED,path,not isdir)
            else:
                self.notify_watchers(CREATED,path)
        for dst_path,isdir in sorted(dst_paths.items()):
            path = pathjoin(dst,dst_path)
            if not self.wrapped_fs.exists(path):
                self.notify_watchers(REMOVED,path)

    def _post_move(self,src,dst,data):
        (src_paths,dst_paths) = data
        for src_path,isdir in sorted(src_paths.items(),reverse=True):
            path = pathjoin(src,src_path)
            self.notify_watchers(REMOVED,path)

    def setxattr(self,path,name,value):
        super(WatchableFS,self).setxattr(path,name,value)
        self.notify_watchers(MODIFIED,path,False)

    def delxattr(self,path,name):
        super(WatchableFS,self).delxattr(path,name)
        self.notify_watchers(MODIFIED,path,False)



class PollingWatchableFS(WatchableFS):
    """FS wrapper simulating watcher callbacks by periodic polling.

    This FS wrapper augments the functionality of WatchableFS by periodically
    polling the underlying FS for changes.  It is thus capable of detecting
    changes made to the underlying FS via other interfaces, albeit with a
    (configurable) delay to account for the polling interval.
    """

    def __init__(self,wrapped_fs,poll_interval=60*5):
        super(PollingWatchableFS,self).__init__(wrapped_fs)
        self.poll_interval = poll_interval
        self.add_watcher(self._on_path_modify,"/",(CREATED,MOVED_DST,))
        self.add_watcher(self._on_path_modify,"/",(MODIFIED,ACCESSED,))
        self.add_watcher(self._on_path_delete,"/",(REMOVED,MOVED_SRC,))
        self._path_info = PathMap()
        self._poll_thread = threading.Thread(target=self._poll_for_changes)
        self._poll_cond = threading.Condition()
        self._poll_close_event = threading.Event()
        self._poll_thread.start()

    def close(self):
        self._poll_close_event.set()
        self._poll_thread.join()
        super(PollingWatchableFS,self).close()

    def _on_path_modify(self,event):
        path = event.path
        try:
            try:
                self._path_info[path] = self.wrapped_fs.getinfo(path)
            except ResourceNotFoundError:
                self._path_info.clear(path)
        except FSError:
            pass

    def _on_path_delete(self,event):
        self._path_info.clear(event.path)

    def _poll_for_changes(self):
        try:
            while not self._poll_close_event.isSet():
                #  Walk all directories looking for changes.
                #  Come back to any that give us an error.
                error_paths = set()
                for dirnm in self.wrapped_fs.walkdirs():
                    if self._poll_close_event.isSet():
                        break
                    try:
                        self._check_for_changes(dirnm)
                    except FSError:
                        error_paths.add(dirnm)
                #  Retry the directories that gave us an error, until
                #  we have successfully updated them all
                while error_paths and not self._poll_close_event.isSet():
                    dirnm = error_paths.pop()
                    if self.wrapped_fs.isdir(dirnm):
                        try:
                            self._check_for_changes(dirnm)
                        except FSError:
                            error_paths.add(dirnm)
                #  Notify that we have completed a polling run
                self._poll_cond.acquire()
                self._poll_cond.notifyAll()
                self._poll_cond.release()
                #  Sleep for the specified interval, or until closed.
                self._poll_close_event.wait(timeout=self.poll_interval)
        except FSError:
            if not self.closed:
                raise

    def _check_for_changes(self,dirnm):
        #  Check the metadata for the directory itself.
        new_info = self.wrapped_fs.getinfo(dirnm)
        try:
            old_info = self._path_info[dirnm]
        except KeyError:
            self.notify_watchers(CREATED,dirnm)
        else:
            if new_info != old_info:
                self.notify_watchers(MODIFIED,dirnm,False)
        #  Check the metadata for each file in the directory.
        #  We assume that if the file's data changes, something in its
        #  metadata will also change; don't want to read through each file!
        #  Subdirectories will be handled by the outer polling loop.
        for filenm in self.wrapped_fs.listdir(dirnm,files_only=True):
            if self._poll_close_event.isSet():
                return
            fpath = pathjoin(dirnm,filenm)
            new_info = self.wrapped_fs.getinfo(fpath)
            try:
                old_info = self._path_info[fpath]
            except KeyError:
                self.notify_watchers(CREATED,fpath)
            else:
                was_accessed = False
                was_modified = False
                for (k,v) in new_info.iteritems():
                    if k not in old_info:
                        was_modified = True
                        break
                    elif old_info[k] != v:
                        if k in ("accessed_time","st_atime",):
                            was_accessed = True
                        elif k:
                            was_modified = True
                            break
                else:
                    for k in old_info:
                        if k not in new_info:
                            was_modified = True
                            break
                if was_modified:
                    self.notify_watchers(MODIFIED,fpath,True)
                elif was_accessed:
                    self.notify_watchers(ACCESSED,fpath)
        #  Check for deletion of cached child entries.
        for childnm in self._path_info.iternames(dirnm):
            if self._poll_close_event.isSet():
                return
            cpath = pathjoin(dirnm,childnm)
            if not self.wrapped_fs.exists(cpath):
                self.notify_watchers(REMOVED,cpath)



def ensure_watchable(fs,wrapper_class=PollingWatchableFS,*args,**kwds):
    """Ensure that the given fs supports watching, simulating it if necessary.

    Given an FS object, this function returns an equivalent FS that has support
    for watcher callbacks.  This may be the original object if it supports them
    natively, or a wrapper class if they must be simulated.
    """
    if isinstance(fs,wrapper_class):
        return fs
    try:
        w = fs.add_watcher(lambda e: None,"/",recursive=False)
    except (AttributeError,FSError):
        return wrapper_class(fs,*args,**kwds)
    else:
        fs.del_watcher(w)
    return fs


class iter_changes(object):
    """Blocking iterator over the change events produced by an FS.

    This class can be used to transform the callback-based watcher mechanism
    into a blocking stream of events.  It operates by having the callbacks
    push events onto a queue as they come in, then reading them off one at a
    time.
    """

    def __init__(self,fs=None,path="/",events=None,**kwds):
        self.closed = False
        self._queue = Queue.Queue()
        self._watching = set()
        if fs is not None:
            self.add_watcher(fs,path,events,**kwds)

    def __iter__(self):
        return self

    def __del__(self):
        self.close()

    def next(self,timeout=None):
        if not self._watching:
            raise StopIteration
        try:
            event = self._queue.get(timeout=timeout)
        except Queue.Empty:
            raise StopIteration
        if event is None:
            raise StopIteration
        if isinstance(event,CLOSED):
            event.fs.del_watcher(self._enqueue)
            self._watching.remove(event.fs)
        return event

    def close(self):
        if not self.closed:
            self.closed = True
            for fs in self._watching:
                fs.del_watcher(self._enqueue)
            self._queue.put(None)

    def add_watcher(self,fs,path="/",events=None,**kwds):
        w = fs.add_watcher(self._enqueue,path,events,**kwds)
        self._watching.add(fs)
        return w

    def _enqueue(self,event):
        self._queue.put(event)

    def del_watcher(self,watcher):
        for fs in self._watching:
            try:
                fs.del_watcher(watcher)
                break
            except ValueError:
                pass
        else:
            raise ValueError("watcher not found: %s" % (watcher,))


