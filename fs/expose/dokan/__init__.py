"""
fs.expose.dokan
===============

Expose an FS object to the native filesystem via Dokan.

This module provides the necessary interfaces to mount an FS object into
the local filesystem using Dokan on win32::

    http://dokan-dev.github.io/

For simple usage, the function 'mount' takes an FS object
and new device mount point or an existing empty folder
and exposes the given FS as that path::

    >>> from fs.memoryfs import MemoryFS
    >>> from fs.expose import dokan
    >>> fs = MemoryFS()
    >>> # Mount device mount point
    >>> mp = dokan.mount(fs, "Q:\\")
    >>> mp.path
    'Q:\\'
    >>> mp.unmount()
    >>> fs = MemoryFS()
    >>> # Mount in an existing empty folder.
    >>> mp = dokan.mount(fs, "C:\\test")
    >>> mp.path
    'C:\\test'
    >>> mp.unmount()

The above spawns a new background process to manage the Dokan event loop, which
can be controlled through the returned subprocess.Popen object.  To avoid
spawning a new process, set the 'foreground' option::

    >>> #  This will block until the filesystem is unmounted
    >>> dokan.mount(fs, "Q:\\", foreground=True)

Any additional options for the Dokan process can be passed as keyword arguments
to the 'mount' function.

If you require finer control over the creation of the Dokan process, you can
instantiate the MountProcess class directly.  It accepts all options available
to subprocess.Popen::

    >>> from subprocess import PIPE
    >>> mp = dokan.MountProcess(fs, "Q:\\", stderr=PIPE)
    >>> dokan_errors = mp.communicate()[1]


If you are exposing an untrusted filesystem, you may like to apply the
wrapper class Win32SafetyFS before passing it into dokan.  This will take
a number of steps to avoid suspicious operations on windows, such as
hiding autorun files.

The binding to Dokan is created via ctypes.  Due to the very stable ABI of
win32, this should work without further configuration on just about all
systems with Dokan installed.

"""
#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  Copyright (c) 2016-2016, Adrien J. <liryna.stark@gmail.com>.
#  All rights reserved; available under the terms of the MIT License.

from __future__ import with_statement, absolute_import

import six
import sys
import os
import errno
import time
import stat as statinfo
import subprocess
try:
    import cPickle as pickle
except ImportError:
    import pickle
import datetime
import ctypes
from collections import deque
from six.moves import range

from fs.base import threading
from fs.errors import *
from fs.path import *
from fs.local_functools import wraps
from fs.wrapfs import WrapFS

try:
    from . import libdokan
except (NotImplementedError, EnvironmentError, ImportError, NameError,):
    is_available = False
    sys.modules.pop("fs.expose.dokan.libdokan", None)
    libdokan = None
else:
    is_available = True
    from ctypes.wintypes import LPCWSTR, WCHAR
    kernel32 = ctypes.windll.kernel32

import logging
logger = logging.getLogger("fs.expose.dokan")


#  Options controlling the behavior of the Dokan filesystem
#  Ouput debug message
DOKAN_OPTION_DEBUG = 1
#  Ouput debug message to stderr
DOKAN_OPTION_STDERR = 2
#  Use alternate stream
DOKAN_OPTION_ALT_STREAM = 4
#  Mount drive as write-protected.
DOKAN_OPTION_WRITE_PROTECT = 8
#  Use network drive, you need to install Dokan network provider.
DOKAN_OPTION_NETWORK = 16
#  Use removable drive
DOKAN_OPTION_REMOVABLE = 32
#  Use mount manager
DOKAN_OPTION_MOUNT_MANAGER = 64
#  Mount the drive on current session only
DOKAN_OPTION_CURRENT_SESSION = 128
#  FileLock in User Mode
DOKAN_OPTION_FILELOCK_USER_MODE = 256

#  Error codes returned by DokanMain
DOKAN_SUCCESS = 0
#  General Error
DOKAN_ERROR = -1
#  Bad Drive letter
DOKAN_DRIVE_LETTER_ERROR = -2
#  Can't install driver
DOKAN_DRIVER_INSTALL_ERROR = -3
#  Driver something wrong
DOKAN_START_ERROR = -4
#  Can't assign a drive letter or mount point
DOKAN_MOUNT_ERROR = -5
#  Mount point is invalid
DOKAN_MOUNT_POINT_ERROR = -6
#  Requested an incompatible version
DOKAN_VERSION_ERROR = -7

# Misc windows constants
FILE_LIST_DIRECTORY = 0x01
FILE_SHARE_READ = 0x01
FILE_SHARE_WRITE = 0x02
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OVERLAPPED = 0x40000000

FILE_ATTRIBUTE_ARCHIVE = 32
FILE_ATTRIBUTE_COMPRESSED = 2048
FILE_ATTRIBUTE_DIRECTORY = 16
FILE_ATTRIBUTE_HIDDEN = 2
FILE_ATTRIBUTE_NORMAL = 128
FILE_ATTRIBUTE_OFFLINE = 4096
FILE_ATTRIBUTE_READONLY = 1
FILE_ATTRIBUTE_SYSTEM = 4
FILE_ATTRIBUTE_TEMPORARY = 4

FILE_CREATE = 2
FILE_OPEN = 1
FILE_OPEN_IF = 3
FILE_OVERWRITE = 4
FILE_SUPERSEDE = 0
FILE_OVERWRITE_IF = 5

FILE_GENERIC_READ = 1179785
FILE_GENERIC_WRITE = 1179926

FILE_DELETE_ON_CLOSE = 0x00001000

REQ_GENERIC_READ = 0x80 | 0x08 | 0x01
REQ_GENERIC_WRITE = 0x004 | 0x0100 | 0x002 | 0x0010

STATUS_SUCCESS = 0x0
STATUS_ACCESS_DENIED = 0xC0000022
STATUS_LOCK_NOT_GRANTED = 0xC0000055
STATUS_NOT_SUPPORTED = 0xC00000BB
STATUS_OBJECT_NAME_COLLISION = 0xC0000035
STATUS_DIRECTORY_NOT_EMPTY = 0xC0000101
STATUS_NOT_LOCKED = 0xC000002A
STATUS_OBJECT_NAME_NOT_FOUND = 0xC0000034
STATUS_NOT_IMPLEMENTED = 0xC0000002
STATUS_OBJECT_PATH_NOT_FOUND = 0xC000003A
STATUS_BUFFER_OVERFLOW = 0x80000005

ERROR_ALREADY_EXISTS = 183

FILE_CASE_SENSITIVE_SEARCH = 0x00000001
FILE_CASE_PRESERVED_NAMES = 0x00000002
FILE_SUPPORTS_REMOTE_STORAGE = 0x00000100
FILE_UNICODE_ON_DISK = 0x00000004
FILE_PERSISTENT_ACLS = 0x00000008

#  Some useful per-process global information
NATIVE_ENCODING = sys.getfilesystemencoding()

DATETIME_ZERO = datetime.datetime(1, 1, 1, 0, 0, 0)
DATETIME_STARTUP = datetime.datetime.utcnow()

FILETIME_UNIX_EPOCH = 116444736000000000


def handle_fs_errors(func):
    """Method decorator to report FS errors in the appropriate way.

    This decorator catches all FS errors and translates them into an
    equivalent OSError, then returns the negated error number.  It also
    makes the function return zero instead of None as an indication of
    successful execution.
    """
    func = convert_fs_errors(func)

    @wraps(func)
    def wrapper(*args, **kwds):
        try:
            res = func(*args, **kwds)
        except OSError as e:
            if e.errno:
                res = _errno2syserrcode(e.errno)
            else:
                res = STATUS_ACCESS_DENIED;
        except Exception as e:
            raise
        else:
            if res is None:
                res = 0
        return res
    return wrapper


# During long-running operations, Dokan requires that the DokanResetTimeout
# function be called periodically to indicate the progress is still being
# made.  Unfortunately we don't have any facility for the underlying FS
# to make these calls for us, so we have to hack around it.
#
# The idea is to use a single background thread to monitor all active Dokan
# method calls, resetting the timeout until they have completed.  Note that
# this completely undermines the point of DokanResetTimeout as it's now
# possible for a deadlock to hang the entire filesystem.

_TIMEOUT_PROTECT_THREAD = None
_TIMEOUT_PROTECT_LOCK = threading.Lock()
_TIMEOUT_PROTECT_COND = threading.Condition(_TIMEOUT_PROTECT_LOCK)
_TIMEOUT_PROTECT_QUEUE = deque()
_TIMEOUT_PROTECT_WAIT_TIME = 4 * 60
_TIMEOUT_PROTECT_RESET_TIME = 5 * 60 * 1000


def _start_timeout_protect_thread():
    """Start the background thread used to protect dokan from timeouts.

    This function starts the background thread that monitors calls into the
    dokan API and resets their timeouts.  It's safe to call this more than
    once, only a single thread will be started.
    """
    global _TIMEOUT_PROTECT_THREAD
    with _TIMEOUT_PROTECT_LOCK:
        if _TIMEOUT_PROTECT_THREAD is None:
            target = _run_timeout_protect_thread
            _TIMEOUT_PROTECT_THREAD = threading.Thread(target=target)
            _TIMEOUT_PROTECT_THREAD.daemon = True
            _TIMEOUT_PROTECT_THREAD.start()


def _run_timeout_protect_thread():
    while True:
        with _TIMEOUT_PROTECT_COND:
            try:
                (when, info, finished) = _TIMEOUT_PROTECT_QUEUE.popleft()
            except IndexError:
                _TIMEOUT_PROTECT_COND.wait()
                continue
        if finished:
            continue
        now = time.time()
        wait_time = max(0, _TIMEOUT_PROTECT_WAIT_TIME - now + when)
        time.sleep(wait_time)
        with _TIMEOUT_PROTECT_LOCK:
            if finished:
                continue
            libdokan.DokanResetTimeout(_TIMEOUT_PROTECT_RESET_TIME, info)
            _TIMEOUT_PROTECT_QUEUE.append((now + wait_time, info, finished))


def timeout_protect(func):
    """Method decorator to enable timeout protection during call.

    This decorator adds an entry to the timeout protect queue before executing
    the function, and marks it as finished when the function exits.
    """
    @wraps(func)
    def wrapper(self, *args):
        if _TIMEOUT_PROTECT_THREAD is None:
            _start_timeout_protect_thread()
        info = args[-1]
        finished = []
        try:
            with _TIMEOUT_PROTECT_COND:
                _TIMEOUT_PROTECT_QUEUE.append((time.time(), info, finished))
                _TIMEOUT_PROTECT_COND.notify()
            return func(self, *args)
        finally:
            with _TIMEOUT_PROTECT_LOCK:
                finished.append(True)
    return wrapper


MIN_FH = 100


class FSOperations(object):
    """Object delegating all DOKAN_OPERATIONS pointers to an FS object."""

    def __init__(self, fs, fsname="NTFS", volname="Dokan Volume", securityfolder=os.path.expanduser('~')):
        if libdokan is None:
            msg = 'dokan library (http://dokan-dev.github.io/) is not available'
            raise OSError(msg)
        self.fs = fs
        self.fsname = fsname
        self.volname = volname
        self.securityfolder = securityfolder
        self._files_by_handle = {}
        self._files_lock = threading.Lock()
        self._next_handle = MIN_FH
        #  Windows requires us to implement a kind of "lazy deletion", where
        #  a handle is marked for deletion but this is not actually done
        #  until the handle is closed.  This set monitors pending deletes.
        self._pending_delete = set()
        #  Since pyfilesystem has no locking API, we manage file locks
        #  in memory.  This maps paths to a list of current locks.
        self._active_locks = PathMap()
        #  Dokan expects a succesful write() to be reflected in the file's
        #  reported size, but the FS might buffer writes and prevent this.
        #  We explicitly keep track of the size Dokan expects a file to be.
        #  This dict is indexed by path, then file handle.
        self._files_size_written = PathMap()

    def get_ops_struct(self):
        """Get a DOKAN_OPERATIONS struct mapping to our methods."""
        struct = libdokan.DOKAN_OPERATIONS()
        for (nm, typ) in libdokan.DOKAN_OPERATIONS._fields_:
            setattr(struct, nm, typ(getattr(self, nm)))
        return struct

    def _get_file(self, fh):
        """Get the information associated with the given file handle."""
        try:
            return self._files_by_handle[fh]
        except KeyError:
            raise FSError("invalid file handle")

    def _reg_file(self, f, path):
        """Register a new file handle for the given file and path."""
        self._files_lock.acquire()
        try:
            fh = self._next_handle
            self._next_handle += 1
            lock = threading.Lock()
            self._files_by_handle[fh] = (f, path, lock)
            if path not in self._files_size_written:
                self._files_size_written[path] = {}
            self._files_size_written[path][fh] = 0
            return fh
        finally:
            self._files_lock.release()

    def _rereg_file(self, fh, f):
        """Re-register the file handle for the given file.

        This might be necessary if we are required to write to a file
        after its handle was closed (e.g. to complete an async write).
        """
        self._files_lock.acquire()
        try:
            (f2, path, lock) = self._files_by_handle[fh]
            assert f2.closed
            self._files_by_handle[fh] = (f, path, lock)
            return fh
        finally:
            self._files_lock.release()

    def _del_file(self, fh):
        """Unregister the given file handle."""
        self._files_lock.acquire()
        try:
            (f, path, lock) = self._files_by_handle.pop(fh)
            del self._files_size_written[path][fh]
            if not self._files_size_written[path]:
                del self._files_size_written[path]
        finally:
            self._files_lock.release()

    def _is_pending_delete(self, path):
        """Check if the given path is pending deletion.

        This is true if the path or any of its parents have been marked
        as pending deletion, false otherwise.
        """
        for ppath in recursepath(path):
            if ppath in self._pending_delete:
                return True
        return False

    def _check_lock(self, path, offset, length, info, locks=None):
        """Check whether the given file range is locked.

        This method implements basic lock checking.  It checks all the locks
        held against the given file, and if any overlap the given byte range
        then it returns STATUS_LOCK_NOT_GRANTED.  If the range is not locked, it will
        return zero.
        """
        if locks is None:
            with self._files_lock:
                try:
                    locks = self._active_locks[path]
                except KeyError:
                    return STATUS_SUCCESS
        for (lh, lstart, lend) in locks:
            if info is not None and info.contents.Context == lh:
                continue
            if lstart >= offset + length:
                continue
            if lend <= offset:
                continue
            return STATUS_LOCK_NOT_GRANTED
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def ZwCreateFile(self, path, securitycontext, access, attribute, sharing, disposition, options, info):
        path = self._dokanpath2pyfs(path)
        #  Can't open files that are pending delete.
        if self._is_pending_delete(path):
            return STATUS_ACCESS_DENIED

        retcode = STATUS_SUCCESS
        if self.fs.isdir(path) or info.contents.IsDirectory:
            info.contents.IsDirectory = True
            exist = self.fs.exists(path)
            if disposition == FILE_CREATE:
                if self.fs.exists(path):
                    retcode = STATUS_OBJECT_NAME_COLLISION
                self.fs.makedir(path)
            elif disposition == FILE_OPEN_IF:
                if not self.fs.exists(path):
                    retcode = STATUS_OBJECT_PATH_NOT_FOUND
        else:
            # If no access rights are requestsed, only basic metadata is queried.
            if not access:
                if self.fs.isdir(path):
                    info.contents.IsDirectory = True
                elif not self.fs.exists(path):
                    return STATUS_OBJECT_NAME_NOT_FOUND
                return STATUS_SUCCESS
            #  This is where we'd convert the access mask into an appropriate
            #  mode string.  Unfortunately, I can't seem to work out all the
            #  details.  I swear MS Word is trying to write to files that it
            #  opens without asking for write permission.
            #  For now, just set the mode based on disposition flag.
            if disposition == FILE_OVERWRITE_IF or disposition == FILE_SUPERSEDE:
                if self.fs.exists(path):
                    retcode = STATUS_OBJECT_NAME_COLLISION
                mode = "w+b"
            elif disposition == FILE_OPEN_IF:
                if not self.fs.exists(path):
                    mode = "w+b"
                else:
                    mode = "r+b"
            elif disposition == FILE_OPEN:
                if not self.fs.exists(path):
                    return STATUS_OBJECT_NAME_NOT_FOUND
                mode = "r+b"
            elif disposition == FILE_OVERWRITE:
                if not self.fs.exists(path):
                    return STATUS_OBJECT_NAME_NOT_FOUND
                mode = "w+b"
            elif disposition == FILE_CREATE:
                if self.fs.exists(path):
                    return STATUS_OBJECT_NAME_COLLISION
                mode = "w+b"
            else:
                mode = "r+b"
            #  Try to open the requested file.  It may actually be a directory.
            info.contents.Context = 1
            try:
                f = self.fs.open(path, mode)
                #  print(path, mode, repr(f))
            except ResourceInvalidError:
                info.contents.IsDirectory = True
            except FSError as e:
                #  Sadly, win32 OSFS will raise all kinds of strange errors
                #  if you try to open() a directory.  Need to check by hand.
                if self.fs.isdir(path):
                    info.contents.IsDirectory = True
                else:
                    # print(e)
                    raise
            else:
                info.contents.Context = self._reg_file(f, path)
            if retcode == STATUS_SUCCESS and (options & FILE_DELETE_ON_CLOSE):
                self._pending_delete.add(path)
        return retcode

    @timeout_protect
    @handle_fs_errors
    def Cleanup(self, path, info):
        path = self._dokanpath2pyfs(path)
        if info.contents.IsDirectory:
            if info.contents.DeleteOnClose:
                self.fs.removedir(path)
                self._pending_delete.remove(path)
        else:
            (file, _, lock) = self._get_file(info.contents.Context)
            lock.acquire()
            try:
                file.close()
                if info.contents.DeleteOnClose:
                    self.fs.remove(path)
                    self._pending_delete.remove(path)
                    self._del_file(info.contents.Context)
                    info.contents.Context = 0
            finally:
                lock.release()

    @timeout_protect
    @handle_fs_errors
    def CloseFile(self, path, info):
        if info.contents.Context >= MIN_FH:
            (file, _, lock) = self._get_file(info.contents.Context)
            lock.acquire()
            try:
                file.close()
                self._del_file(info.contents.Context)
            finally:
                lock.release()
            info.contents.Context = 0

    @timeout_protect
    @handle_fs_errors
    def ReadFile(self, path, buffer, nBytesToRead, nBytesRead, offset, info):
        path = self._dokanpath2pyfs(path)
        (file, _, lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            status = self._check_lock(path, offset, nBytesToRead, info)
            if status:
                return status
            #  This may be called after Cleanup, meaning we
            #  need to re-open the file.
            if file.closed:
                file = self.fs.open(path, file.mode)
                self._rereg_file(info.contents.Context, file)
            file.seek(offset)
            data = file.read(nBytesToRead)
            ctypes.memmove(buffer, ctypes.create_string_buffer(data), len(data))
            nBytesRead[0] = len(data)
        finally:
            lock.release()
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def WriteFile(self, path, buffer, nBytesToWrite, nBytesWritten, offset, info):
        path = self._dokanpath2pyfs(path)
        fh = info.contents.Context
        (file, _, lock) = self._get_file(fh)
        lock.acquire()
        try:
            status = self._check_lock(path, offset, nBytesToWrite, info)
            if status:
                return status
            #  This may be called after Cleanup, meaning we
            #  need to re-open the file.
            if file.closed:
                file = self.fs.open(path, file.mode)
                self._rereg_file(info.contents.Context, file)
            if info.contents.WriteToEndOfFile:
                file.seek(0, os.SEEK_END)
            else:
                file.seek(offset)
            data = ctypes.create_string_buffer(nBytesToWrite)
            ctypes.memmove(data, buffer, nBytesToWrite)
            file.write(data.raw)
            nBytesWritten[0] = len(data.raw)
            try:
                size_written = self._files_size_written[path][fh]
            except KeyError:
                pass
            else:
                if offset + nBytesWritten[0] > size_written:
                    new_size_written = offset + nBytesWritten[0]
                    self._files_size_written[path][fh] = new_size_written
        finally:
            lock.release()
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def FlushFileBuffers(self, path, info):
        path = self._dokanpath2pyfs(path)
        (file, _, lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            file.flush()
        finally:
            lock.release()
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def GetFileInformation(self, path, buffer, info):
        path = self._dokanpath2pyfs(path)
        finfo = self.fs.getinfo(path)
        data = buffer.contents
        self._info2finddataw(path, finfo, data, info)
        try:
            written_size = max(self._files_size_written[path].values())
        except KeyError:
            pass
        else:
            reported_size = (data.nFileSizeHigh << 32) + data.nFileSizeLow
            if written_size > reported_size:
                data.nFileSizeHigh = written_size >> 32
                data.nFileSizeLow = written_size & 0xffffffff
        data.nNumberOfLinks = 1
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def FindFiles(self, path, fillFindData, info):
        path = self._dokanpath2pyfs(path)
        for (nm, finfo) in self.fs.listdirinfo(path):
            fpath = pathjoin(path, nm)
            if self._is_pending_delete(fpath):
                continue
            data = self._info2finddataw(fpath, finfo)
            fillFindData(ctypes.byref(data), info)
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def FindFilesWithPattern(self, path, pattern, fillFindData, info):
        path = self._dokanpath2pyfs(path)
        for (nm, finfo) in self.fs.listdirinfo(path):
            fpath = pathjoin(path, nm)
            if self._is_pending_delete(fpath):
                continue
            if not libdokan.DokanIsNameInExpression(pattern, nm, True):
                continue
            data = self._info2finddataw(fpath, finfo, None)
            fillFindData(ctypes.byref(data), info)
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def SetFileAttributes(self, path, attrs, info):
        path = self._dokanpath2pyfs(path)
        # TODO: decode various file attributes
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def SetFileTime(self, path, ctime, atime, mtime, info):
        path = self._dokanpath2pyfs(path)
        # setting ctime is not supported
        if atime is not None:
            try:
                atime = _filetime2datetime(atime.contents)
            except ValueError:
                atime = None
        if mtime is not None:
            try:
                mtime = _filetime2datetime(mtime.contents)
            except ValueError:
                mtime = None
        #  some programs demand this succeed; fake it
        try:
            self.fs.settimes(path, atime, mtime)
        except UnsupportedError:
            pass
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def DeleteFile(self, path, info):
        path = self._dokanpath2pyfs(path)
        if not self.fs.isfile(path):
            if not self.fs.exists(path):
                return STATUS_ACCESS_DENIED
            else:
                return STATUS_OBJECT_NAME_NOT_FOUND
        self._pending_delete.add(path)
        # the actual delete takes place in self.CloseFile()
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def DeleteDirectory(self, path, info):
        path = self._dokanpath2pyfs(path)
        for nm in self.fs.listdir(path):
            if not self._is_pending_delete(pathjoin(path, nm)):
                return STATUS_DIRECTORY_NOT_EMPTY
        self._pending_delete.add(path)
        # the actual delete takes place in self.CloseFile()
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def MoveFile(self, src, dst, overwrite, info):
        #  Close the file if we have an open handle to it.
        if info.contents.Context >= MIN_FH:
            (file, _, lock) = self._get_file(info.contents.Context)
            lock.acquire()
            try:
                file.close()
                self._del_file(info.contents.Context)
            finally:
                lock.release()
        src = self._dokanpath2pyfs(src)
        dst = self._dokanpath2pyfs(dst)
        if info.contents.IsDirectory:
            self.fs.movedir(src, dst, overwrite=overwrite)
        else:
            self.fs.move(src, dst, overwrite=overwrite)
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def SetEndOfFile(self, path, length, info):
        self._dokanpath2pyfs(path)
        (file, _, lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            pos = file.tell()
            if length != pos:
                file.seek(length)
            file.truncate()
            if pos < length:
                file.seek(min(pos, length))
        finally:
            lock.release()
        return STATUS_SUCCESS

    @handle_fs_errors
    def GetDiskFreeSpace(self, nBytesAvail, nBytesTotal, nBytesFree, info):
        #  This returns a stupidly large number if not info is available.
        #  It's better to pretend an operation is possible and have it fail
        #  than to pretend an operation will fail when it's actually possible.
        large_amount = 100 * 1024 * 1024 * 1024
        nBytesFree[0] = self.fs.getmeta("free_space", large_amount)
        nBytesTotal[0] = self.fs.getmeta("total_space", 2 * large_amount)
        nBytesAvail[0] = nBytesFree[0]
        return STATUS_SUCCESS

    @handle_fs_errors
    def GetVolumeInformation(self, vnmBuf, vnmSz, sNum, maxLen, flags, fnmBuf, fnmSz, info):
        nm = ctypes.create_unicode_buffer(self.volname[:vnmSz - 1])
        sz = (len(nm.value) + 1) * ctypes.sizeof(ctypes.c_wchar)
        ctypes.memmove(vnmBuf, nm, sz)
        if sNum:
            sNum[0] = 0
        if maxLen:
            maxLen[0] = 255
        if flags:
            flags[0] = FILE_CASE_SENSITIVE_SEARCH | FILE_CASE_PRESERVED_NAMES | FILE_SUPPORTS_REMOTE_STORAGE | FILE_UNICODE_ON_DISK | FILE_PERSISTENT_ACLS;
        nm = ctypes.create_unicode_buffer(self.fsname[:fnmSz - 1])
        sz = (len(nm.value) + 1) * ctypes.sizeof(ctypes.c_wchar)
        ctypes.memmove(fnmBuf, nm, sz)
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def SetAllocationSize(self, path, length, info):
        #  I think this is supposed to reserve space for the file
        #  but *not* actually move the end-of-file marker.
        #  No way to do that in pyfs.
        return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def LockFile(self, path, offset, length, info):
        end = offset + length
        with self._files_lock:
            try:
                locks = self._active_locks[path]
            except KeyError:
                locks = self._active_locks[path] = []
            else:
                status = self._check_lock(path, offset, length, None, locks)
                if status:
                    return status
            locks.append((info.contents.Context, offset, end))
            return STATUS_SUCCESS

    @timeout_protect
    @handle_fs_errors
    def UnlockFile(self, path, offset, length, info):
        with self._files_lock:
            try:
                locks = self._active_locks[path]
            except KeyError:
                return STATUS_NOT_LOCKED
            todel = []
            for i, (lh, lstart, lend) in enumerate(locks):
                if info.contents.Context == lh:
                    if lstart == offset:
                        if lend == offset + length:
                            todel.append(i)
            if not todel:
                return STATUS_NOT_LOCKED
            for i in reversed(todel):
                del locks[i]
            return STATUS_SUCCESS

    @handle_fs_errors
    def GetFileSecurity(self, path, securityinformation, securitydescriptor, securitydescriptorlength, neededlength, info):
        securitydescriptor = ctypes.cast(securitydescriptor, libdokan.PSECURITY_DESCRIPTOR)
        path = self._dokanpath2pyfs(path)
        if self.fs.isdir(path):
            res = libdokan.GetFileSecurity(
                self.securityfolder,
                ctypes.cast(securityinformation, libdokan.PSECURITY_INFORMATION)[0],
                securitydescriptor,
                securitydescriptorlength,
                neededlength,
            )
            return STATUS_SUCCESS if res else STATUS_BUFFER_OVERFLOW
        return STATUS_NOT_IMPLEMENTED

    @handle_fs_errors
    def SetFileSecurity(self, path, securityinformation, securitydescriptor, securitydescriptorlength, info):
        return STATUS_NOT_IMPLEMENTED

    @handle_fs_errors
    def Mounted(self, info):
        return STATUS_SUCCESS

    @handle_fs_errors
    def Unmounted(self, info):
        return STATUS_SUCCESS

    @handle_fs_errors
    def FindStreams(self, path, callback, info):
        return STATUS_NOT_IMPLEMENTED

    def _dokanpath2pyfs(self, path):
        path = path.replace('\\', '/')
        return normpath(path)

    def _info2attrmask(self, path, info, hinfo=None):
        """Convert a file/directory info dict to a win32 file attribute mask."""
        attrs = 0
        st_mode = info.get("st_mode", None)
        if st_mode:
            if statinfo.S_ISDIR(st_mode):
                attrs |= FILE_ATTRIBUTE_DIRECTORY
            elif statinfo.S_ISREG(st_mode):
                attrs |= FILE_ATTRIBUTE_NORMAL
        if not attrs and hinfo:
            if hinfo.contents.IsDirectory:
                attrs |= FILE_ATTRIBUTE_DIRECTORY
            else:
                attrs |= FILE_ATTRIBUTE_NORMAL
        if not attrs:
            if self.fs.isdir(path):
                attrs |= FILE_ATTRIBUTE_DIRECTORY
            else:
                attrs |= FILE_ATTRIBUTE_NORMAL
        return attrs

    def _info2finddataw(self, path, info, data=None, hinfo=None):
        """Convert a file/directory info dict into a WIN32_FIND_DATAW struct."""
        if data is None:
            data = libdokan.WIN32_FIND_DATAW()
        data.dwFileAttributes = self._info2attrmask(path, info, hinfo)
        data.ftCreationTime = _datetime2filetime(info.get("created_time", None))
        data.ftLastAccessTime = _datetime2filetime(info.get("accessed_time", None))
        data.ftLastWriteTime = _datetime2filetime(info.get("modified_time", None))
        data.nFileSizeHigh = info.get("size", 0) >> 32
        data.nFileSizeLow = info.get("size", 0) & 0xffffffff
        data.cFileName = basename(path)
        data.cAlternateFileName = ""
        return data


def _datetime2timestamp(dtime):
    """Convert a datetime object to a unix timestamp."""
    t = time.mktime(dtime.timetuple())
    t += dtime.microsecond / 1000000.0
    return t


def _timestamp2datetime(tstamp):
    """Convert a unix timestamp to a datetime object."""
    return datetime.datetime.fromtimestamp(tstamp)


def _timestamp2filetime(tstamp):
    f = FILETIME_UNIX_EPOCH + int(tstamp * 10000000)
    return libdokan.FILETIME(f & 0xffffffff, f >> 32)


def _filetime2timestamp(ftime):
    f = ftime.dwLowDateTime | (ftime.dwHighDateTime << 32)
    return (f - FILETIME_UNIX_EPOCH) / 10000000.0


def _filetime2datetime(ftime):
    """Convert a FILETIME struct info datetime.datetime object."""
    if ftime is None:
        return DATETIME_ZERO
    if ftime.dwLowDateTime == 0 and ftime.dwHighDateTime == 0:
        return DATETIME_ZERO
    return _timestamp2datetime(_filetime2timestamp(ftime))


def _datetime2filetime(dtime):
    """Convert a FILETIME struct info datetime.datetime object."""
    if dtime is None:
        return libdokan.FILETIME(0, 0)
    if dtime == DATETIME_ZERO:
        return libdokan.FILETIME(0, 0)
    return _timestamp2filetime(_datetime2timestamp(dtime))


def _errno2syserrcode(eno):
    """Convert an errno into a win32 system error code."""
    if eno == errno.EEXIST:
        return STATUS_OBJECT_NAME_COLLISION
    if eno == errno.ENOTEMPTY:
        return STATUS_DIRECTORY_NOT_EMPTY
    if eno == errno.ENOSYS:
        return STATUS_NOT_SUPPORTED
    if eno == errno.EACCES:
        return STATUS_ACCESS_DENIED
    return eno


def _check_path_string(path):  # TODO Probably os.path has a better check for this...
    """Check path string."""
    if not path or not path[0].isalpha() or not path[1:3] == ':\\':
        raise ValueError("invalid path: %r" % (path,))


def mount(fs, path, foreground=False, ready_callback=None, unmount_callback=None, **kwds):
    """Mount the given FS at the given path, using Dokan.

    By default, this function spawns a new background process to manage the
    Dokan event loop.  The return value in this case is an instance of the
    'MountProcess' class, a subprocess.Popen subclass.

    If the keyword argument 'foreground' is given, we instead run the Dokan
    main loop in the current process.  In this case the function will block
    until the filesystem is unmounted, then return None.

    If the keyword argument 'ready_callback' is provided, it will be called
    when the filesystem has been mounted and is ready for use.  Any additional
    keyword arguments control the behavior of the final dokan mount point.
    Some interesting options include:

        * numthreads:  number of threads to use for handling Dokan requests
        * fsname:  name to display in explorer etc
        * flags:   DOKAN_OPTIONS bitmask
        * securityfolder:  folder path used to duplicate security rights on all folders 
        * FSOperationsClass:  custom FSOperations subclass to use

    """
    if libdokan is None:
        raise OSError("the dokan library is not available")
    _check_path_string(path)
    #  This function captures the logic of checking whether the Dokan mount
    #  is up and running.  Unfortunately I can't find a way to get this
    #  via a callback in the Dokan API.  Instead we just check for the path
    #  in a loop, polling the mount proc to make sure it hasn't died.

    def check_alive(mp):
        if mp and mp.poll() is not None:
            raise OSError("dokan mount process exited prematurely")

    def check_ready(mp=None):
        if ready_callback is not False:
            check_alive(mp)
            for _ in range(100):
                try:
                    os.stat(path)
                except EnvironmentError:
                    check_alive(mp)
                    time.sleep(0.05)
                else:
                    check_alive(mp)
                    if ready_callback:
                        return ready_callback()
                    else:
                        return None
            else:
                check_alive(mp)
                raise OSError("dokan mount process seems to be hung")
    #  Running the the foreground is the final endpoint for the mount
    #  operation, it's where we call DokanMain().
    if foreground:
        numthreads = kwds.pop("numthreads", 0)
        flags = kwds.pop("flags", 0)
        FSOperationsClass = kwds.pop("FSOperationsClass", FSOperations)
        opts = libdokan.DOKAN_OPTIONS(libdokan.DOKAN_MINIMUM_COMPATIBLE_VERSION, numthreads, flags, 0, path, "", 2000, 512, 512)
        ops = FSOperationsClass(fs, **kwds)
        if ready_callback:
            check_thread = threading.Thread(target=check_ready)
            check_thread.daemon = True
            check_thread.start()
        opstruct = ops.get_ops_struct()
        res = libdokan.DokanMain(ctypes.byref(opts), ctypes.byref(opstruct))
        if res != DOKAN_SUCCESS:
            raise OSError("Dokan failed with error: %d" % (res,))
        if unmount_callback:
            unmount_callback()
    #  Running the background, spawn a subprocess and wait for it
    #  to be ready before returning.
    else:
        mp = MountProcess(fs, path, kwds)
        check_ready(mp)
        if unmount_callback:
            orig_unmount = mp.unmount

            def new_unmount():
                orig_unmount()
                unmount_callback()
            mp.unmount = new_unmount
        return mp


def unmount(path):
    """Unmount the given path.

    This function unmounts the dokan path mounted at the given path.
    It works but may leave dangling processes; its better to use the "unmount"
    method on the MountProcess class if you have one.
    """
    _check_path_string(path)
    if not libdokan.DokanRemoveMountPoint(path):
        raise OSError("filesystem could not be unmounted: %s" % (path,))


class MountProcess(subprocess.Popen):
    """subprocess.Popen subclass managing a Dokan mount.

    This is a subclass of subprocess.Popen, designed for easy management of
    a Dokan mount in a background process.  Rather than specifying the command
    to execute, pass in the FS object to be mounted, the target path
    and a dictionary of options for the Dokan process.

    In order to be passed successfully to the new process, the FS object
    must be pickleable. Since win32 has no fork() this restriction is not
    likely to be lifted (see also the "multiprocessing" module)

    This class has an extra attribute 'path' giving the path of the mounted
    filesystem, and an extra method 'unmount' that will cleanly unmount it
    and terminate the process.
    """

    #  This works by spawning a new python interpreter and passing it the
    #  pickled (fs,path,opts) tuple on the command-line.  Something like this:
    #
    #    python -c "import MountProcess; MountProcess._do_mount('..data..')
    #

    unmount_timeout = 5

    def __init__(self, fs, path, dokan_opts={}, nowait=False, **kwds):
        if libdokan is None:
            raise OSError("the dokan library is not available")
        _check_path_string(path)
        self.path = path
        cmd = "try: import cPickle as pickle;\n"
        cmd = cmd + "except ImportError: import pickle;\n"
        cmd = cmd + "data = pickle.loads(%s); "
        cmd = cmd + "from fs.expose.dokan import MountProcess; "
        cmd = cmd + "MountProcess._do_mount(data)"
        cmd = cmd % (repr(pickle.dumps((fs, path, dokan_opts, nowait), -1)),)
        cmd = [sys.executable, "-c", cmd]
        super(MountProcess, self).__init__(cmd, **kwds)

    def unmount(self):
        """Cleanly unmount the Dokan filesystem, terminating this subprocess."""
        if not libdokan.DokanRemoveMountPoint(self.path):
            raise OSError("the filesystem could not be unmounted: %s" %(self.path,))
        self.terminate()

    if not hasattr(subprocess.Popen, "terminate"):
        def terminate(self):
            """Gracefully terminate the subprocess."""
            kernel32.TerminateProcess(int(self._handle), -1)

    if not hasattr(subprocess.Popen, "kill"):
        def kill(self):
            """Forcibly terminate the subprocess."""
            kernel32.TerminateProcess(int(self._handle), -1)

    @staticmethod
    def _do_mount(data):
        """Perform the specified mount."""
        (fs, path, opts, nowait) = data
        opts["foreground"] = True

        def unmount_callback():
            fs.close()
        opts["unmount_callback"] = unmount_callback
        if nowait:
            opts["ready_callback"] = False
        mount(fs, path, **opts)


class Win32SafetyFS(WrapFS):
    """FS wrapper for extra safety when mounting on win32.

    This wrapper class provides some safety features when mounting untrusted
    filesystems on win32.  Specifically:

        * hiding autorun files
        * removing colons from paths

    """

    def __init__(self, wrapped_fs, allow_autorun=False):
        self.allow_autorun = allow_autorun
        super(Win32SafetyFS, self).__init__(wrapped_fs)

    def _encode(self, path):
        path = relpath(normpath(path))
        path = path.replace(":", "__colon__")
        if not self.allow_autorun:
            if path.lower().startswith("_autorun."):
                path = path[1:]
        return path

    def _decode(self, path):
        path = relpath(normpath(path))
        path = path.replace("__colon__", ":")
        if not self.allow_autorun:
            if path.lower().startswith("autorun."):
                path = "_" + path
        return path


if __name__ == "__main__":
    import os.path
    import tempfile
    from fs.osfs import OSFS
    from fs.memoryfs import MemoryFS
    from shutil import rmtree
    from six import b
    path = tempfile.mkdtemp()
    try:
        fs = OSFS(path)
        #fs = MemoryFS()
        fs.setcontents("test1.txt", b("test one"))
        flags = DOKAN_OPTION_DEBUG | DOKAN_OPTION_STDERR | DOKAN_OPTION_REMOVABLE
        mount(fs, "Q:\\", foreground=True, numthreads=1, flags=flags)
        fs.close()
    finally:
        rmtree(path)
