"""
fs.osfs.watch_win32
===================

Change watcher support for OSFS, using ReadDirectoryChangesW on win32.

"""

import os
import sys
import errno
import threading
import Queue
import stat
import struct
import ctypes
import ctypes.wintypes
import traceback
import weakref

try:
    LPVOID = ctypes.wintypes.LPVOID
except AttributeError:
    # LPVOID wasn't defined in Py2.5, guess it was introduced in Py2.6
    LPVOID = ctypes.c_void_p

from fs.errors import *
from fs.path import *
from fs.watch import *


INVALID_HANDLE_VALUE = 0xFFFFFFFF

FILE_NOTIFY_CHANGE_FILE_NAME = 0x01
FILE_NOTIFY_CHANGE_DIR_NAME = 0x02
FILE_NOTIFY_CHANGE_ATTRIBUTES = 0x04
FILE_NOTIFY_CHANGE_SIZE = 0x08
FILE_NOTIFY_CHANGE_LAST_WRITE = 0x010
FILE_NOTIFY_CHANGE_LAST_ACCESS = 0x020
FILE_NOTIFY_CHANGE_CREATION = 0x040
FILE_NOTIFY_CHANGE_SECURITY = 0x0100

FILE_LIST_DIRECTORY = 0x01
FILE_SHARE_READ = 0x01
FILE_SHARE_WRITE = 0x02
OPEN_EXISTING = 3
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OVERLAPPED = 0x40000000

THREAD_TERMINATE = 0x0001

FILE_ACTION_ADDED = 1
FILE_ACTION_REMOVED = 2
FILE_ACTION_MODIFIED = 3
FILE_ACTION_RENAMED_OLD_NAME = 4
FILE_ACTION_RENAMED_NEW_NAME = 5
FILE_ACTION_OVERFLOW = 0xFFFF

WAIT_ABANDONED = 0x00000080
WAIT_IO_COMPLETION = 0x000000C0
WAIT_OBJECT_0 = 0x00000000
WAIT_TIMEOUT = 0x00000102


def _errcheck_bool(value,func,args):
    if not value:
        raise ctypes.WinError()
    return args

def _errcheck_handle(value,func,args):
    if not value:
        raise ctypes.WinError()
    if value == INVALID_HANDLE_VALUE:
        raise ctypes.WinError()
    return args

def _errcheck_dword(value,func,args):
    if value == 0xFFFFFFFF:
        raise ctypes.WinError()
    return args


class OVERLAPPED(ctypes.Structure):
    _fields_ = [('Internal', LPVOID),
                ('InternalHigh', LPVOID),
                ('Offset', ctypes.wintypes.DWORD),
                ('OffsetHigh', ctypes.wintypes.DWORD),
                ('Pointer', LPVOID),
                ('hEvent', ctypes.wintypes.HANDLE),
               ]


try:
    ReadDirectoryChangesW = ctypes.windll.kernel32.ReadDirectoryChangesW
except AttributeError:
    raise ImportError("ReadDirectoryChangesW is not available")
ReadDirectoryChangesW.restype = ctypes.wintypes.BOOL
ReadDirectoryChangesW.errcheck = _errcheck_bool
ReadDirectoryChangesW.argtypes = (
    ctypes.wintypes.HANDLE, # hDirectory
    LPVOID, # lpBuffer
    ctypes.wintypes.DWORD, # nBufferLength
    ctypes.wintypes.BOOL, # bWatchSubtree
    ctypes.wintypes.DWORD, # dwNotifyFilter
    ctypes.POINTER(ctypes.wintypes.DWORD), # lpBytesReturned
    ctypes.POINTER(OVERLAPPED), # lpOverlapped
    LPVOID #FileIOCompletionRoutine # lpCompletionRoutine
)

CreateFileW = ctypes.windll.kernel32.CreateFileW
CreateFileW.restype = ctypes.wintypes.HANDLE
CreateFileW.errcheck = _errcheck_handle
CreateFileW.argtypes = (
    ctypes.wintypes.LPCWSTR, # lpFileName
    ctypes.wintypes.DWORD, # dwDesiredAccess
    ctypes.wintypes.DWORD, # dwShareMode
    LPVOID, # lpSecurityAttributes
    ctypes.wintypes.DWORD, # dwCreationDisposition
    ctypes.wintypes.DWORD, # dwFlagsAndAttributes
    ctypes.wintypes.HANDLE # hTemplateFile
)

CloseHandle = ctypes.windll.kernel32.CloseHandle
CloseHandle.restype = ctypes.wintypes.BOOL
CloseHandle.argtypes = (
    ctypes.wintypes.HANDLE, # hObject
)

CreateEvent = ctypes.windll.kernel32.CreateEventW
CreateEvent.restype = ctypes.wintypes.HANDLE
CreateEvent.errcheck = _errcheck_handle
CreateEvent.argtypes = (
    LPVOID, # lpEventAttributes
    ctypes.wintypes.BOOL, # bManualReset
    ctypes.wintypes.BOOL, # bInitialState
    ctypes.wintypes.LPCWSTR, #lpName
)

SetEvent = ctypes.windll.kernel32.SetEvent
SetEvent.restype = ctypes.wintypes.BOOL
SetEvent.errcheck = _errcheck_bool
SetEvent.argtypes = (
    ctypes.wintypes.HANDLE, # hEvent
)

WaitForSingleObjectEx = ctypes.windll.kernel32.WaitForSingleObjectEx
WaitForSingleObjectEx.restype = ctypes.wintypes.DWORD
WaitForSingleObjectEx.errcheck = _errcheck_dword
WaitForSingleObjectEx.argtypes = (
    ctypes.wintypes.HANDLE, # hObject
    ctypes.wintypes.DWORD, # dwMilliseconds
    ctypes.wintypes.BOOL, # bAlertable
)

CreateIoCompletionPort = ctypes.windll.kernel32.CreateIoCompletionPort
CreateIoCompletionPort.restype = ctypes.wintypes.HANDLE
CreateIoCompletionPort.errcheck = _errcheck_handle
CreateIoCompletionPort.argtypes = (
    ctypes.wintypes.HANDLE, # FileHandle
    ctypes.wintypes.HANDLE, # ExistingCompletionPort
    LPVOID, # CompletionKey
    ctypes.wintypes.DWORD, # NumberOfConcurrentThreads
)

GetQueuedCompletionStatus = ctypes.windll.kernel32.GetQueuedCompletionStatus
GetQueuedCompletionStatus.restype = ctypes.wintypes.BOOL
GetQueuedCompletionStatus.errcheck = _errcheck_bool
GetQueuedCompletionStatus.argtypes = (
    ctypes.wintypes.HANDLE, # CompletionPort
    LPVOID, # lpNumberOfBytesTransferred
    LPVOID, # lpCompletionKey
    ctypes.POINTER(OVERLAPPED), # lpOverlapped
    ctypes.wintypes.DWORD, # dwMilliseconds
)

PostQueuedCompletionStatus = ctypes.windll.kernel32.PostQueuedCompletionStatus
PostQueuedCompletionStatus.restype = ctypes.wintypes.BOOL
PostQueuedCompletionStatus.errcheck = _errcheck_bool
PostQueuedCompletionStatus.argtypes = (
    ctypes.wintypes.HANDLE, # CompletionPort
    ctypes.wintypes.DWORD, # lpNumberOfBytesTransferred
    ctypes.wintypes.DWORD, # lpCompletionKey
    ctypes.POINTER(OVERLAPPED), # lpOverlapped
)



class WatchedDirectory(object):

    def __init__(self,callback,path,flags,recursive=True):
        self.path = path
        self.flags = flags
        self.callback = callback
        self.recursive = recursive
        self.handle = None
        self.error = None
        self.handle = CreateFileW(path,
                          FILE_LIST_DIRECTORY,
                          FILE_SHARE_READ | FILE_SHARE_WRITE,
                          None,
                          OPEN_EXISTING,
                          FILE_FLAG_BACKUP_SEMANTICS|FILE_FLAG_OVERLAPPED,
                          None)
        self.result = ctypes.create_string_buffer(1024)
        self.overlapped = overlapped = OVERLAPPED()
        self.ready = threading.Event()

    def __del__(self):
        self.close()

    def close(self):
        if self.handle is not None:
            CloseHandle(self.handle)
            self.handle = None

    def post(self):
        overlapped = self.overlapped
        overlapped.Internal = 0
        overlapped.InternalHigh = 0
        overlapped.Offset = 0
        overlapped.OffsetHigh = 0
        overlapped.Pointer = 0
        overlapped.hEvent = 0
        try:
            ReadDirectoryChangesW(self.handle,
                                  ctypes.byref(self.result),len(self.result),
                                  self.recursive,self.flags,None,
                                  overlapped,None)
        except WindowsError, e:
            self.error = e
            self.close()

    def complete(self,nbytes):
        if nbytes == 0:
            self.callback(None,0)
        else:
            res = self.result.raw[:nbytes]
            for (name,action) in self._extract_change_info(res):
                if self.callback:
                    self.callback(os.path.join(self.path,name),action)

    def _extract_change_info(self,buffer):
        """Extract the information out of a FILE_NOTIFY_INFORMATION structure."""
        pos = 0
        while pos < len(buffer):
            jump, action, namelen = struct.unpack("iii",buffer[pos:pos+12])
            # TODO: this may return a shortname or a longname, with no way
            # to tell which.  Normalise them somehow?
            name = buffer[pos+12:pos+12+namelen].decode("utf16")
            yield (name,action)
            if not jump:
                break
            pos += jump


class WatchThread(threading.Thread):
    """Thread for watching filesystem changes."""

    def __init__(self):
        super(WatchThread,self).__init__()
        self.closed = False
        self.watched_directories = {}
        self.ready = threading.Event()
        self._iocp = None
        self._new_watches = Queue.Queue()

    def close(self):
        if not self.closed:
            self.closed = True
            if self._iocp:
                PostQueuedCompletionStatus(self._iocp,0,1,None)

    def add_watcher(self,callback,path,events,recursive):
        if os.path.isfile(path):
            path = os.path.dirname(path)
        watched_dirs = []
        for w in self._get_watched_dirs(callback,path,events,recursive):
            self.attach_watched_directory(w)
            watched_dirs.append(w)
        return watched_dirs

    def del_watcher(self,w):
        w = self.watched_directories.pop(hash(w))
        w.callback = None
        w.close()

    def _get_watched_dirs(self,callback,path,events,recursive):
        do_access = False
        do_change = False
        flags = 0
        for evt in events:
            if issubclass(ACCESSED,evt):
                do_access = True
            if issubclass(MODIFIED,evt):
                do_change = True
                flags |= FILE_NOTIFY_CHANGE_ATTRIBUTES
                flags |= FILE_NOTIFY_CHANGE_CREATION
                flags |= FILE_NOTIFY_CHANGE_SECURITY
            if issubclass(CREATED,evt):
                flags |= FILE_NOTIFY_CHANGE_FILE_NAME
                flags |= FILE_NOTIFY_CHANGE_DIR_NAME
            if issubclass(REMOVED,evt):
                flags |= FILE_NOTIFY_CHANGE_FILE_NAME
                flags |= FILE_NOTIFY_CHANGE_DIR_NAME
            if issubclass(MOVED_SRC,evt):
                flags |= FILE_NOTIFY_CHANGE_FILE_NAME
                flags |= FILE_NOTIFY_CHANGE_DIR_NAME
            if issubclass(MOVED_DST,evt):
                flags |= FILE_NOTIFY_CHANGE_FILE_NAME
                flags |= FILE_NOTIFY_CHANGE_DIR_NAME
        if do_access:
            # Separately capture FILE_NOTIFY_CHANGE_LAST_ACCESS events
            # so we can reliably generate ACCESSED events.
            def on_access_event(path,action):
                if action == FILE_ACTION_OVERFLOW:
                    callback(OVERFLOW,path)
                else:
                    callback(ACCESSED,path)
            yield WatchedDirectory(on_access_event,path,
                                   FILE_NOTIFY_CHANGE_LAST_ACCESS,recursive)
        if do_change:
            # Separately capture FILE_NOTIFY_CHANGE_LAST_WRITE events
            # so we can generate MODIFIED(data_changed=True) events.
            cflags = FILE_NOTIFY_CHANGE_LAST_WRITE | FILE_NOTIFY_CHANGE_SIZE
            def on_change_event(path,action):
                if action == FILE_ACTION_OVERFLOW:
                    callback(OVERFLOW,path)
                else:
                    callback(MODIFIED,path,True)
            yield WatchedDirectory(on_change_event,path,cflags,recursive)
        if flags:
            #  All other events we can route through a common handler.
            old_name = [None]
            def on_misc_event(path,action):
                if action == FILE_ACTION_OVERFLOW:
                    callback(OVERFLOW,path)
                elif action == FILE_ACTION_ADDED:
                    callback(CREATED,path)
                elif action == FILE_ACTION_REMOVED:
                    callback(REMOVED,path)
                elif action == FILE_ACTION_MODIFIED:
                    callback(MODIFIED,path)
                elif action == FILE_ACTION_RENAMED_OLD_NAME:
                    old_name[0] = path
                elif action == FILE_ACTION_RENAMED_NEW_NAME:
                    callback(MOVED_DST,path,old_name[0])
                    callback(MOVED_SRC,old_name[0],path)
                    old_name[0] = None
            yield WatchedDirectory(on_misc_event,path,flags,recursive)

    def run(self):
        try:
            self._iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE,None,0,1)
            self.ready.set()
            nbytes = ctypes.wintypes.DWORD()
            iocpkey = ctypes.wintypes.DWORD()
            overlapped = OVERLAPPED()
            while not self.closed:
                try:
                    GetQueuedCompletionStatus(self._iocp,
                                              ctypes.byref(nbytes),
                                              ctypes.byref(iocpkey),
                                              ctypes.byref(overlapped),
                                              -1)
                except WindowsError:
                    traceback.print_exc()
                else:
                    if iocpkey.value > 1:
                        try:
                            w = self.watched_directories[iocpkey.value]
                        except KeyError:
                            pass
                        else:
                            w.complete(nbytes.value)
                            w.post()
                    elif not self.closed:
                        try:
                            while True:
                                w = self._new_watches.get_nowait()
                                if w.handle is not None:
                                    CreateIoCompletionPort(w.handle,
                                                           self._iocp,
                                                           hash(w),0)
                                    w.post()
                                w.ready.set()
                        except Queue.Empty:
                            pass
        finally:
            self.ready.set()
            for w in self.watched_directories.itervalues():
                w.close()
            if self._iocp:
                CloseHandle(self._iocp)

    def attach_watched_directory(self,w):
        self.watched_directories[hash(w)] = w
        self._new_watches.put(w)
        PostQueuedCompletionStatus(self._iocp,0,1,None)
        w.ready.wait()


class OSFSWatchMixin(WatchableFSMixin):
    """Mixin providing change-watcher support via pyinotify."""

    __watch_lock = threading.Lock()
    __watch_thread = None

    def close(self):
        super(OSFSWatchMixin,self).close()
        self.__shutdown_watch_thread(force=True)
        self.notify_watchers(CLOSED)

    @convert_os_errors
    def add_watcher(self,callback,path="/",events=None,recursive=True):
        w = super(OSFSWatchMixin,self).add_watcher(callback,path,events,recursive)
        syspath = self.getsyspath(path)
        wt = self.__get_watch_thread()
        #  Careful not to create a reference cycle here.
        weak_self = weakref.ref(self)
        def handle_event(event_class,path,*args,**kwds):
            selfref = weak_self()
            if selfref is None:
                return
            try:
                path = selfref.unsyspath(path)
            except ValueError:
                pass
            else:
                if event_class in (MOVED_SRC,MOVED_DST) and args and args[0]:
                    args = (selfref.unsyspath(args[0]),) + args[1:]
                event = event_class(selfref,path,*args,**kwds)
                w.handle_event(event)
        w._watch_objs = wt.add_watcher(handle_event,syspath,w.events,w.recursive)
        for wd in w._watch_objs:
            if wd.error is not None:
                self.del_watcher(w)
                raise wd.error
        return w

    @convert_os_errors
    def del_watcher(self,watcher_or_callback):
        wt = self.__get_watch_thread()
        if isinstance(watcher_or_callback,Watcher):
            watchers = [watcher_or_callback]
        else:
            watchers = self._find_watchers(watcher_or_callback)
        for watcher in watchers:
            for wobj in watcher._watch_objs:
                wt.del_watcher(wobj)
            super(OSFSWatchMixin,self).del_watcher(watcher)
        if not wt.watched_directories:
            self.__shutdown_watch_thread()

    def __get_watch_thread(self):
        """Get the shared watch thread, initializing if necessary."""
        if self.__watch_thread is None:
            self.__watch_lock.acquire()
            try:
                if self.__watch_thread is None:
                    wt = WatchThread()
                    wt.start()
                    wt.ready.wait()
                    OSFSWatchMixin.__watch_thread = wt
            finally:
                self.__watch_lock.release()
        return self.__watch_thread

    def __shutdown_watch_thread(self,force=False):
        """Stop the shared watch manager, if there are no watches left."""
        self.__watch_lock.acquire()
        try:
            if OSFSWatchMixin.__watch_thread is None:
                return
            if not force and OSFSWatchMixin.__watch_thread.watched_directories:
                return
            try:
                OSFSWatchMixin.__watch_thread.close()
            except EnvironmentError:
                pass
            else:
                OSFSWatchMixin.__watch_thread.join()
            OSFSWatchMixin.__watch_thread = None
        finally:
            self.__watch_lock.release()


