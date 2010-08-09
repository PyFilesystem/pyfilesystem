"""
fs.expose.dokan
===============

Expose an FS object to the native filesystem via Dokan.

This module provides the necessary interfaces to mount an FS object into
the local filesystem using Dokan on win32::

    http://dokan-dev.net/en/

For simple usage, the function 'mount' takes an FS object and a drive letter,
and exposes the given FS as that drive::

    >>> from fs.memoryfs import MemoryFS
    >>> from fs.expose import dokan
    >>> fs = MemoryFS()
    >>> mp = dokan.mount(fs,"Q")
    >>> mp.drive
    'Q'
    >>> mp.path
    'Q:\\'
    >>> mp.unmount()

The above spawns a new background process to manage the Dokan event loop, which
can be controlled through the returned subprocess.Popen object.  To avoid
spawning a new process, set the 'foreground' option::

    >>> #  This will block until the filesystem is unmounted
    >>> dokan.mount(fs,"Q",foreground=True)

Any additional options for the Dokan process can be passed as keyword arguments
to the 'mount' function.

If you require finer control over the creation of the Dokan process, you can
instantiate the MountProcess class directly.  It accepts all options available
to subprocess.Popen::

    >>> from subprocess import PIPE
    >>> mp = dokan.MountProcess(fs,"Q",stderr=PIPE)
    >>> dokan_errors = mp.communicate()[1]

The binding to Dokan is created via ctypes.  Due to the very stable ABI of
win32, this should work without further configuration on just about all
systems with Dokan installed.

"""
#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  All rights reserved; available under the terms of the MIT License.

import sys
if sys.platform != "win32":
    raise ImportError("Dokan is only available on win32")

import os
import signal
import errno
import time
import stat as statinfo
import subprocess
import pickle
import datetime
import ctypes
from ctypes.wintypes import LPCWSTR, WCHAR

kernel32 = ctypes.windll.kernel32

from fs.base import threading
from fs.errors import *
from fs.path import *
from fs.functools import wraps

try:
    import libdokan
except NotImplementedError:
    raise ImportError("Dokan found but not usable")


DokanMain = libdokan.DokanMain
DokanOperations = libdokan.DokanOperations

#  Options controlling the behaiour of the Dokan filesystem
DOKAN_OPTION_DEBUG = 1
DOKAN_OPTION_STDERR = 2
DOKAN_OPTION_ALT_STREAM = 4
DOKAN_OPTION_KEEP_ALIVE = 8
DOKAN_OPTION_NETWORK = 16
DOKAN_OPTION_REMOVABLE = 32

#  Error codes returned by DokanMain
DOKAN_SUCCESS = 0
DOKAN_ERROR = -1
DOKAN_DRIVE_LETTER_ERROR = -2
DOKAN_DRIVER_INSTALL_ERROR = -3
DOKAN_START_ERROR = -4
DOKAN_MOUNT_ERROR = -5

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

CREATE_NEW = 1
CREATE_ALWAYS = 2
OPEN_EXISTING = 3
OPEN_ALWAYS = 4
TRUNCATE_EXISTING = 5

GENERIC_READ = 128
GENERIC_WRITE = 1180054

#  Some useful per-process global information
NATIVE_ENCODING = sys.getfilesystemencoding()

DATETIME_ZERO = datetime.datetime(1,1,1,0,0,0)
DATETIME_STARTUP = datetime.datetime.utcnow()

FILETIME_UNIX_EPOCH = 116444736000000000


def _debug(*args):
    #print args; sys.stdout.flush()
    pass


def handle_fs_errors(func):
    """Method decorator to report FS errors in the appropriate way.

    This decorator catches all FS errors and translates them into an
    equivalent OSError, then returns the negated error number.  It also
    makes the function return zero instead of None as an indication of
    successful execution.
    """
    name = func.__name__
    func = convert_fs_errors(func)
    @wraps(func)
    def wrapper(*args,**kwds):        
        _debug("CALL",name,args[1:-1])
        try:
            res = func(*args,**kwds)
        except OSError, e:
            _debug("ERR",name,e)
            if e.errno:
                res = -1 * _errno2syserrcode(e.errno)
            else:
                res = -1
        except Exception, e:
            _debug("ERR",name,e)
            raise
        else:
            if res is None:
                res = 0
        _debug("RES",name,res)
        return res
    return wrapper
 


MIN_FH = 100

class FSOperations(DokanOperations):
    """DokanOperations interface delegating all activities to an FS object."""

    def __init__(self, fs, on_init=None, on_unmount=None, fsname="Dokan FS", volname="Dokan Volume"):
        super(FSOperations,self).__init__()
        self.fs = fs
        self.fsname = fsname
        self.volname = volname
        self._on_init = on_init
        self._on_unmount = on_unmount
        self._files_by_handle = {}
        self._files_lock = threading.Lock()
        self._next_handle = MIN_FH
        self._pending_delete = set()
        #  TODO: do we need this for dokan?  It's a hangover from FUSE.
        #  Dokan expects a succesful write() to be reflected in the file's
        #  reported size, but the FS might buffer writes and prevent this.
        #  We explicitly keep track of the size FUSE expects a file to be.
        #  This dict is indexed by path, then file handle.
        self._files_size_written = {}

    def _get_file(self, fh):
        try:
            return self._files_by_handle[fh]
        except KeyError:
            raise FSError("invalid file handle")

    def _reg_file(self, f, path):
        self._files_lock.acquire()
        try:
            fh = self._next_handle
            self._next_handle += 1
            lock = threading.Lock()
            self._files_by_handle[fh] = (f,path,lock)
            if path not in self._files_size_written:
                self._files_size_written[path] = {}
            self._files_size_written[path][fh] = 0
            return fh
        finally:
            self._files_lock.release()

    def _del_file(self, fh):
        self._files_lock.acquire()
        try:
            (f,path,lock) = self._files_by_handle.pop(fh)
            del self._files_size_written[path][fh]
            if not self._files_size_written[path]:
                del self._files_size_written[path] 
        finally:
            self._files_lock.release()

    def _is_pending_delete(self,path):
        for ppath in recursepath(path):
            if ppath in self._pending_delete:
                return True
        return False

    def Unmount(self, info):
        if self._on_unmount:
            self._on_unmount()

    @handle_fs_errors
    def CreateFile(self, path, access, sharing, disposition, flags, info):
        path = normpath(path)
        #  Can't open files that are pending delete.
        if self._is_pending_delete(path):
            return -ERROR_ACCESS_DENIED
        # If no access rights are requestsed, only basic metadata is queried.
        if not access:
            if self.fs.isdir(path):
                info.contents.IsDirectory = True
            elif not self.fs.exists(path):
                raise ResourceNotFoundError(path)
            return
        # Convert the various access rights into an appropriate mode string.
        # TODO: I'm sure this misses some important semantics.
        retcode = 0
        if access & GENERIC_READ:
            if access & GENERIC_WRITE:
                if disposition == CREATE_ALWAYS:
                    if self.fs.exists(path):
                        retcode = 183
                    mode = "w+b"
                elif disposition == OPEN_EXISTING:
                    mode = "r+b"
                elif disposition == TRUNCATE_EXISTING:
                    if not self.fs.exists(path):
                        raise ResourceNotFoundError(path)
                    mode = "w+b"
                elif disposition == CREATE_NEW:
                    if self.fs.exists(path):
                        return -183
                    mode = "w+b"
                else:
                    mode = "r+b"
            else:
                mode = "rb"
        else:
            if disposition == CREATE_ALWAYS:
                if self.fs.exists(path):
                    retcode = 183
                mode = "wb"
            elif disposition == OPEN_EXISTING:
                if self.fs.exists(path):
                    retcode = 183
                mode = "r+b"
            elif disposition == TRUNCATE_EXISTING:
                if not self.fs.exists(path):
                    raise ResourceNotFoundError(path)
                mode = "w+b"
            elif disposition == CREATE_NEW:
                if self.fs.exists(path):
                    return -183
                mode = "w+b"
            else:
                mode = "r+b"
        #  Try to open the requested file.  It may actually be a directory.
        info.contents.Context = 1
        try:
            f = self.fs.open(path,mode)
        except ResourceInvalidError:
            info.contents.IsDirectory = True
        except FSError:
            #  Sadly, win32 OSFS will raise all kinds of strange errors
            #  if you try to open() a directory.  Need to check by hand.
            if self.fs.isdir(path):
                info.contents.IsDirectory = True
            else:
                raise
        else:
            info.contents.Context = self._reg_file(f,path)
        return retcode

    @handle_fs_errors
    def OpenDirectory(self, path, info):
        path = normpath(path)
        if self._is_pending_delete(path):
            raise ResourceNotFoundError(path)
        if not self.fs.isdir(path):
             if not self.fs.exists(path):
                 raise ResourceNotFoundError(path)
             else:
                 raise ResourceInvalidError(path)
        info.contents.IsDirectory = True

    @handle_fs_errors
    def CreateDirectory(self, path, info):
        path = normpath(path)
        if self._is_pending_delete(path):
            return -ERROR_ACCESS_DENIED
        self.fs.makedir(path)
        info.contents.IsDirectory = True

    @handle_fs_errors
    def Cleanup(self, path, info):
        path = normpath(path)
        if info.contents.DeleteOnClose:
            if info.contents.IsDirectory:
                self.fs.removedir(path)
                self._pending_delete.remove(path)
            else:
                (file,_,lock) = self._get_file(info.contents.Context)
                lock.acquire()
                try:
                    file.close()
                    self.fs.remove(path)
                    self._pending_delete.remove(path)
                    self._del_file(info.contents.Context)
                finally:
                    lock.release()
        elif info.contents.Context >= MIN_FH:
            (file,_,lock) = self._get_file(info.contents.Context)
            lock.acquire()
            try:
                file.close()
                self._del_file(info.contents.Context)
            finally:
                lock.release()
        info.contents.Context = 0

    @handle_fs_errors
    def CloseFile(self, path, info):
        if info.contents.Context != 0:
            raise FSError("file handle not cleaned up: %s" % (path,))

    @handle_fs_errors
    def ReadFile(self, path, buffer, nBytesToRead, nBytesRead, offset, info):
        path = normpath(path)
        (file,_,lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            file.seek(offset)
            data = file.read(nBytesToRead)
            ctypes.memmove(buffer,ctypes.create_string_buffer(data),len(data))
            nBytesRead[0] = len(data)
        finally:
            lock.release()

    @handle_fs_errors
    def WriteFile(self, path, buffer, nBytesToWrite, nBytesWritten, offset, info):
        path = normpath(path)
        (file,_,lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            if info.contents.WriteToEndOfFile:
                file.seek(0,os.SEEK_END)
            else:
                file.seek(offset)
            data = ctypes.create_string_buffer(nBytesToWrite)
            ctypes.memmove(data,buffer,nBytesToWrite)
            file.write(data.raw)
            nBytesWritten[0] = len(data.raw)
        finally:
            lock.release()

    @handle_fs_errors
    def FlushFileBuffers(self, path, offset, info):
        path = normpath(path)
        (file,_,lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            file.flush()
        finally:
            lock.release()

    @handle_fs_errors
    def GetFileInformation(self, path, buffer, info):
        path = normpath(path)
        info = self.fs.getinfo(path)
        if "name" not in info:
            info["name"] = basename(path)
        data = buffer.contents
        _info2finddataw(info,data)

    @handle_fs_errors
    def FindFiles(self, path, fillFindData, info):
        path = normpath(path)
        for nm in self.fs.listdir(path):
            fpath = pathjoin(path,nm)
            if self._is_pending_delete(fpath):
                continue
            data = _info2finddataw(self.fs.getinfo(fpath))
            fillFindData(ctypes.byref(data),info)

    @handle_fs_errors
    def FindFilesWithPattern(self, path, pattern, fillFindData, info):
        path = normpath(path)
        infolist = []
        try:
            for finfo in self.fs.listdir(path,info=True):
                nm = finfo["name"]
                if self._is_pending_delete(pathjoin(path,nm)):
                    continue
                if not libdokan.DokanIsNameInExpression(pattern,nm,True):
                    continue
                infolist.append(finfo)
        except (TypeError,KeyError,UnsupportedError):
            filtered = True
            for nm in self.fs.listdir(path):
                if self._is_pending_delete(pathjoin(path,nm)):
                    continue
                if not libdokan.DokanIsNameInExpression(pattern,nm,True):
                    continue
                finfo = self.fs.getinfo(pathjoin(path,nm))
                finfo["name"] = nm
                infolist.append(finfo)
        for finfo in infolist:
            data = _info2finddataw(finfo)
            fillFindData(ctypes.byref(data),info)

    @handle_fs_errors
    def SetFileAttributes(self, path, attrs, info):
        path = normpath(path)
        raise UnsupportedError
        # TODO: decode various file attributes

    @handle_fs_errors
    def SetFileTime(self, path, ctime, atime, mtime, info):
        path = normpath(path)
        # setting ctime is not supported
        if atime is not None:
            atime = _filetime2datetime(atime.contents)
        if mtime is  not None:
            mtime = _filetime2datetime(mtime.contents)
        self.fs.settimes(path, atime, mtime)

    @handle_fs_errors
    def DeleteFile(self, path, info):
        path = normpath(path)
        if not self.fs.isfile(path):
            if not self.fs.exists(path):
                raise ResourceNotFoundError(path)
            else:
                raise ResourceInvalidError(path)
        self._pending_delete.add(path)
        # the actual delete takes place in self.CloseFile()

    @handle_fs_errors
    def DeleteDirectory(self, path, info):
        path = normpath(path)
        for nm in self.fs.listdir(path):
            if not self._is_pending_delete(pathjoin(path,nm)):
                raise DirectoryNotEmptyError(path)
        self._pending_delete.add(path)
        # the actual delete takes place in self.CloseFile()

    @handle_fs_errors
    def MoveFile(self, src, dst, overwrite, info):
        #  Close the file if we have an open handle to it.
        if info.contents.Context >= MIN_FH:
            (file,_,lock) = self._get_file(info.contents.Context)
            lock.acquire()
            try:
                file.close()
                self._del_file(info.contents.Context)
            finally:
                lock.release()
        src = normpath(src)
        dst = normpath(dst)
        if info.contents.IsDirectory:
            self.fs.movedir(src,dst,overwrite=overwrite)
        else:
            self.fs.move(src,dst,overwrite=overwrite)

    @handle_fs_errors
    def SetEndOfFile(self, path, length, info):
        path = normpath(path)
        (file,_,lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            file.truncate(length)
        finally:
            lock.release()

    @handle_fs_errors
    def GetDiskFreeSpaceEx(self, nBytesAvail, nBytesTotal, nBytesFree, info):
        nBytesAvail[0] = 8 * 1024 * 1024
        nBytesTotal[0] = 20 * 1024 * 1024
        nBytesFree[0] = 12 * 1024 * 1024

    @handle_fs_errors
    def GetVolumeInformation(self, vnmBuf, vnmSz, sNum, maxLen, flags, fnmBuf, fnmSz, info):
        nm = ctypes.create_unicode_buffer(self.volname[:vnmSz-1])
        sz = (len(nm.value)+1) * ctypes.sizeof(ctypes.c_wchar)
        ctypes.memmove(vnmBuf,nm,sz)
        if sNum:
            sNum[0] = 0
        if maxLen:
            maxLen[0] = 255
        if flags:
            flags[0] = 0
        nm = ctypes.create_unicode_buffer(self.fsname[:fnmSz-1])
        sz = (len(nm.value)+1) * ctypes.sizeof(ctypes.c_wchar)
        ctypes.memmove(fnmBuf,nm,sz)

def _info2attrmask(info):
    """Convert a file/directory info dict to a win32 file attributes mask."""
    attrs = 0
    st_mode = info.get("st_mode",None)
    if st_mode:
        if statinfo.S_ISDIR(st_mode):
            attrs |= FILE_ATTRIBUTE_DIRECTORY
        elif statinfo.S_ISREG(st_mode):
            attrs |= FILE_ATTRIBUTE_NORMAL
    return attrs

def _info2finddataw(info,data=None):
    """Convert a file/directory info dict into a WIN32_FIND_DATAW struct."""
    if data is None:
        data = libdokan.WIN32_FIND_DATAW()
    data.dwFileAttributes = _info2attrmask(info)
    data.ftCreateTime = _datetime2filetime(info.get("created_time",None))
    data.ftAccessTime = _datetime2filetime(info.get("accessed_time",None))
    data.ftWriteTime = _datetime2filetime(info.get("modified_time",None))
    data.nFileSizeHigh = info.get("size",0) >> 32
    data.nFileSizeLow = info.get("size",0) & 0xffffff
    data.cFileName = info.get("name","")
    data.cAlternateFileName = ""
    return data


def _datetime2timestamp(dtime):
    """Convert a datetime object to a unix timestamp."""
    t = time.mktime(dtime.timetuple())
    t += dtime.microsecond / 1000000.0
    return t

DATETIME_LOCAL_TO_UTC = _datetime2timestamp(datetime.datetime.utcnow()) - _datetime2timestamp(datetime.datetime.now())

def _timestamp2datetime(tstamp):
    """Convert a unix timestamp to a datetime object."""
    return datetime.datetime.fromtimestamp(tstamp)

def _timestamp2filetime(tstamp):
    f = FILETIME_UNIX_EPOCH + int(t * 10000000)
    return libdokan.FILETIME(f & 0xffffffff,f >> 32)

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
        return libdokan.FILETIME(0,0)
    if dtime == DATETIME_ZERO:
        return libdokan.FILETIME(0,0)
    return _timestamp2filetime(_datetime2timestamp(dtime))


d = datetime.datetime.now()
t = _datetime2timestamp(d)
f = _datetime2filetime(d)
assert d == _timestamp2datetime(t)
assert t == _filetime2timestamp(_timestamp2filetime(t))
assert d == _filetime2datetime(f)

ERROR_FILE_EXISTS = 80
ERROR_DIR_NOT_EMPTY = 145
ERROR_NOT_SUPPORTED = 50
ERROR_ACCESS_DENIED = 5

def _errno2syserrcode(eno):
    """Convert an errno into a win32 system error code."""
    if eno == errno.EEXIST:
        return ERROR_FILE_EXISTS
    if eno == errno.ENOTEMPTY:
        return ERROR_DIR_NOT_EMPTY
    if eno == errno.ENOSYS:
        return ERROR_NOT_SUPPORTED
    if eno == errno.EACCES:
        return ERROR_ACCESS_DENIED
    return eno


def _normalise_drive_string(drive):
    """Normalise drive string to a single letter."""
    if not drive:
        raise ValueError("invalid drive letter: %r" % (drive,))
    if len(drive) > 3:
        raise ValueError("invalid drive letter: %r" % (drive,))
    if not drive[0].isalpha():
        raise ValueError("invalid drive letter: %r" % (drive,))
    if not ":\\".startswith(drive[1:]):
        raise ValueError("invalid drive letter: %r" % (drive,))
    return drive[0].upper()
    

def mount(fs, drive, foreground=False, ready_callback=None, unmount_callback=None, **kwds):
    """Mount the given FS at the given drive letter, using Dokan.

    By default, this function spawns a new background process to manage the
    Dokan event loop.  The return value in this case is an instance of the
    'MountProcess' class, a subprocess.Popen subclass.

    If the keyword argument 'foreground' is given, we instead run the Dokan
    main loop in the current process.  In this case the function will block
    until the filesystem is unmounted, then return None.

    If the keyword argument 'ready_callback' is provided, it will be called
    when the filesystem has been mounted and is ready for use.  Any additional
    keyword arguments control the behaviour of the final dokan mount point.
    Some interesting options include:

        * numthreads:  number of threads to use for handling Dokan requests
        * fsname:  name to display in explorer etc
        * flags:   DOKAN_OPTIONS bitmask
        * FSOperationsClass:  custom FSOperations subclass to use

    """
    drive = _normalise_drive_string(drive)
    #  This function captures the logic of checking whether the Dokan mount
    #  is up and running.  Unfortunately I can't find a way to get this
    #  via a callback in the Dokan API.  Instead we just check for the drive
    #  in a loop, polling the mount proc to make sure it hasn't died.
    def check_alive(mp):
        if mp and mp.poll() != None:
            raise OSError("dokan mount process exited prematurely")
    def check_ready(mp=None):
        if ready_callback is not False:
            check_alive(mp)
            for _ in xrange(100):
                try:
                    os.stat(drive+":\\")
                except EnvironmentError, e:
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
        numthreads = kwds.pop("numthreads",0)
        flags = kwds.pop("flags",0)
        FSOperationsClass = kwds.pop("FSOperationsClass",FSOperations)
        opts = libdokan.DOKAN_OPTIONS(drive[:1], numthreads, flags)
        ops = FSOperationsClass(fs, on_unmount=unmount_callback, **kwds)
        if ready_callback:
            check_thread = threading.Thread(target=check_ready)
            check_thread.daemon = True
            check_thread.start()
        res = DokanMain(ctypes.byref(opts),ctypes.byref(ops.buffer))
        if res != DOKAN_SUCCESS:
            raise OSError("Dokan failed with error: %d" % (res,))
    #  Running the background, spawn a subprocess and wait for it
    #  to be ready before returning.
    else:
        mp = MountProcess(fs, drive, kwds)
        check_ready(mp)
        if unmount_callback:
            orig_unmount = mp.unmount
            def new_unmount():
                orig_unmount()
                unmount_callback()
            mp.unmount = new_unmount
        return mp


def unmount(drive):
    """Unmount the given drive.

    This function unmounts the dokan drive mounted at the given drive letter.
    It works but may leave dangling processes; its better to use the "unmount"
    method on the MountProcess class if you have one.
    """
    if not libdokan.DokanUnmount(drive):
        raise OSError("filesystem could not be unmounted: %s" % (drive,))


class MountProcess(subprocess.Popen):
    """subprocess.Popen subclass managing a Dokan mount.

    This is a subclass of subprocess.Popen, designed for easy management of
    a Dokan mount in a background process.  Rather than specifying the command
    to execute, pass in the FS object to be mounted, the target drive letter
    and a dictionary of options for the Dokan process.

    In order to be passed successfully to the new process, the FS object
    must be pickleable. Since win32 has no fork() this restriction is not
    likely to be lifted (see also the "multiprcessing" module)

    This class has an extra attribute 'drive' giving the drive of the mounted
    filesystem, and an extra method 'unmount' that will cleanly unmount it
    and terminate the process.
    """

    #  This works by spawning a new python interpreter and passing it the
    #  pickled (fs,path,opts) tuple on the command-line.  Something like this:
    #
    #    python -c "import MountProcess; MountProcess._do_mount('..data..')
    #

    unmount_timeout = 5

    def __init__(self, fs, drive, dokan_opts={}, nowait=False, **kwds):
        self.drive = _normalise_drive_string(drive)
        self.path = self.drive + ":\\"
        cmd = 'from fs.expose.dokan import MountProcess; '
        cmd = cmd + 'MountProcess._do_mount(%s)'
        cmd = cmd % (repr(pickle.dumps((fs,drive,dokan_opts,nowait),-1)),)
        cmd = [sys.executable,"-c",cmd]
        super(MountProcess,self).__init__(cmd,**kwds)

    def unmount(self):
        """Cleanly unmount the Dokan filesystem, terminating this subprocess."""
        if not libdokan.DokanUnmount(self.drive):
            raise OSError("the filesystem could not be unmounted: %s" %(self.drive,))
        self.terminate()

    if not hasattr(subprocess.Popen, "terminate"):
        def terminate(self):
            """Gracefully terminate the subprocess."""
            kernel32.TerminateProcess(int(self._handle),-1)

    if not hasattr(subprocess.Popen, "kill"):
        def kill(self):
            """Forcibly terminate the subprocess."""
            kernel32.TerminateProcess(int(self._handle),-1)

    @staticmethod
    def _do_mount(data):
        """Perform the specified mount."""
        (fs,drive,opts,nowait) = pickle.loads(data)
        opts["foreground"] = True
        def unmount_callback():
            fs.close()
        opts["unmount_callback"] = unmount_callback
        if nowait:
            opts["ready_callback"] = False
        mount(fs,drive,**opts)


if __name__ == "__main__":
    import os, os.path
    import tempfile
    from fs.osfs import OSFS
    from fs.memoryfs import MemoryFS
    path = tempfile.mkdtemp()
    try:
        fs = OSFS(path)
        #fs = MemoryFS()
        fs.setcontents("test1.txt","test one")
        flags = DOKAN_OPTION_DEBUG|DOKAN_OPTION_STDERR|DOKAN_OPTION_REMOVABLE
        mount(fs, "Q", foreground=True, numthreads=1, flags=flags)
        fs.close()
    finally:
        OSFS(path).removedir("/",force=True)


