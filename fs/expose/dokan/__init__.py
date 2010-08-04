"""
fs.expose.dokan
==============

Expose an FS object to the native filesystem via Dokan.

This module provides the necessary interfaces to mount an FS object into
the local filesystem via Dokan::

    http://dokan-dev.net/en/

For simple usage, the function 'mount' takes an FS object and a drive letter,
and exposes the given FS as that drive::

    >>> from fs.memoryfs import MemoryFS
    >>> from fs.expose import dokan
    >>> fs = MemoryFS()
    >>> mp = dokan.mount(fs,"Q")
    >>> mp.drive
    'Q'
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

import os
import sys
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

from fs.base import flags_to_mode, threading
from fs.errors import *
from fs.path import *
from fs.functools import wraps

try:
    import dokan_ctypes as libdokan
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
DATETIME_STARTUP = datetime.datetime.now()


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
        print "CALL", name, args[1:-1]
        try:
            res = func(*args,**kwds)
        except OSError, e:
            if e.errno:
                res = -1 * e.errno
            else:
                res = -1
        else:
            if res is None:
                res = 0
        print "RES:", res
        return res
    return wrapper
 


class FSOperations(DokanOperations):
    """DokanOperations interface delegating all activities to an FS object."""

    def __init__(self, fs, on_init=None, on_unmount=None):
        super(FSOperations,self).__init__()
        self.fs = fs
        self._on_init = on_init
        self._on_unmount = on_unmount
        self._files_by_handle = {}
        self._files_lock = threading.Lock()
        self._next_handle = 100
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

    def Unmount(self, info):
        raise ValueError("HERE")
        if self._on_unmount:
            self._on_unmount()

    @handle_fs_errors
    def CreateFile(self, path, access, sharing, disposition, flags, info):
        path = normpath(path)
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
                else:
                    mode = "ab"
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
                mode = "ab"
            elif disposition == TRUNCATE_EXISTING:
                if not self.fs.exists(path):
                    raise ResourceNotFoundError(path)
                mode = "w+b"
            else:
                mode = "ab"
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
        if not self.fs.isdir(path):
             if not self.fs.exists(path):
                 raise ResourceNotFoundError(path)
             else:
                 raise ResourceInvalidError(path)
        info.contents.IsDirectory = True

    @handle_fs_errors
    def CreateDirectory(self, path, info):
        path = normpath(path)
        self.fs.makedir(path)
        info.contents.IsDirectory = True

    @handle_fs_errors
    def Cleanup(self, path, info):
        path = normpath(path)
        if info.contents.DeleteOnClose:
            if info.contents.IsDirectory:
                self.fs.removedir(path)
            else:
                self.fs.remove(path)

    @handle_fs_errors
    def CloseFile(self, path, info):
        path = normpath(path)
        if info.contents.Context >= 100:
            (file,_,lock) = self._get_file(info.contents.Context)
            lock.acquire()
            try:
                file.close()
                self._del_file(info.contents.Context)
            finally:
                lock.release()

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
            file.seek(offset)
            data = buffer[:nBytesToWrite]
            file.write(data)
            nBytesWritten[0] = len(data)
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
            data = _info2finddataw(self.fs.getinfo(fpath))
            fillFindData(ctypes.byref(data),info)

    @handle_fs_errors
    def FindFilesWithPattern(self, path, pattern, fillFindData, info):
        path = normpath(path)
        infolist = []
        try:
            for finfo in self.fs.listdir(path,info=True):
                nm = finfo["name"]
                if not libdokan.DokanIsNameInExpression(pattern,nm,True):
                    continue
                infolist.append(finfo)
        except (TypeError,KeyError,UnsupportedError):
            filtered = True
            for nm in self.fs.listdir(path):
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
        # TODO: decode various file attributes

    @handle_fs_errors
    def SetFileTime(self, path, ctime, atime, mtime, info):
        path = normpath(path)
        if ctime is not None:
            raise UnsupportedError("cannot set creation time")
        if atime is not None:
            atime = _filetime_to_datetime(atime)
        if mtime is  not None:
            mtime = _filetime_to_datetime(mtime)
        self.fs.settimes(path, atime, mtime, ctime)

    @handle_fs_errors
    def DeleteFile(self, path, info):
        path = normpath(path)
        self.fs.remove(path)

    @handle_fs_errors
    def DeleteDirectory(self, path, info):
        path = normpath(path)
        self.fs.removedir(path)

    @handle_fs_errors
    def MoveFile(self, src, dst, overwrite, info):
        src = normpath(src)
        dst = normpath(dst)
        try:
            self.fs.move(src,dst,overwrite=overwrite)
        except ResourceInvalidError:
            self.fs.movedir(src,dst,overwrite=overwrite)

    @handle_fs_errors
    def SetEndOfFile(self, path, dst, overwrite, info):
        path = normpath(path)
        (file,_,lock) = self._get_file(info.contents.Context)
        lock.acquire()
        try:
            f.truncate()
        finally:
            lock.release()

    @handle_fs_errors
    def GetDiskFreeSpaceEx(self, nBytesAvail, nBytesTotal, nBytesFree, info):
        nBytesAvail[0] = 8 * 1024 * 1024
        nBytesTotal[0] = 20 * 1024 * 1024
        nBytesFree[0] = 12 * 1024 * 1024

    @handle_fs_errors
    def GetVolumeInformation(self, vnmBuf, vnmSz, sNum, maxLen, flags, fnmBuf, fnmSz, info):
        vnmBuf
        vnm = ctypes.create_unicode_buffer("fs.expose.dokan"[:vnmSz-1])
        ctypes.memmove(vnmBuf,vnm,len(vnm.value))
        fnm = ctypes.create_unicode_buffer("fs.expose.dokan"[:fnmSz-1])
        ctypes.memmove(fnmBuf,fnm,len(fnm.value))
        sNum[0] = 0
        maxLen[0] = libdokan.MAX_PATH
        flags = 0

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

def _filetime2datetime(ftime):
    """Convert a FILETIME struct info datetime.datetime object."""
    if ftime is None:
        return DATETIME_ZERO
    if ftime.dwLowDateTime == 0 and ftime.dwHighDateTime == 0:
        return DATETIME_ZERO
    t = ftime.dwLowDateTime & (ftime.dwHighDateTime << 32)
    return datetime.datetime.fromtimestamp(t)

def _datetime2filetime(dtime):
    """Convert a FILETIME struct info datetime.datetime object."""
    if dtime is None:
        return libdokan.FILETIME(0,0)
    if dtime == DATETIME_ZERO:
        return libdokan.FILETIME(0,0)
    # TODO: this doesn't work on win32
    t = int(dtime.strftime("%s"))
    return libdokan.FILETIME(t >> 32,t & 0xffffff)
    

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
    keyword arguments will be passed through as options to the underlying
    Dokan library.  Some interesting options include:

        * numthreads:  number of threads to use for handling Dokan requests
        * fsname:  name to display in explorer etc
        * flags:   DOKAN_OPTIONS bitmask

    """
    def check_ready(mp=None):
        if ready_callback:
            for _ in xrange(100):
                try:
                    os.stat(mp.path)
                except EnvironmentError:
                    time.sleep(0.01)
                else:
                    if mp and mp.poll() != None:
                        raise OSError("dokan mount process exited prematurely")
                    return ready_callback()
    if foreground:
        numthreads = kwds.pop("numthreads",0)
        flags = kwds.pop("flags",0)
        opts = libdokan.DOKAN_OPTIONS(drive, numthreads, flags)
        ops = FSOperations(fs, on_unmount=unmount_callback)
        threading.Thread(target=check_ready).start()
        res = DokanMain(ctypes.byref(opts),ctypes.byref(ops.buffer))
        if res != DOKAN_SUCCESS:
            raise OSError("Dokan failed with error: %d" % (res,))
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

    def __init__(self, fs, drive, dokan_opts={}, **kwds):
        self.drive = drive
        cmd = 'from fs.expose.dokan import MountProcess; '
        cmd = cmd + 'MountProcess._do_mount(%s)'
        cmd = cmd % (repr(pickle.dumps((fs,drive,dokan_opts),-1)),)
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
            kernel32.TerminateProcess(self._handle,-1)

    if not hasattr(subprocess.Popen, "kill"):
        def kill(self):
            """Forcibly terminate the subprocess."""
            kernel32.TerminateProcess(self._handle,-1)

    @staticmethod
    def _do_mount(data):
        """Perform the specified mount."""
        (fs,drive,opts) = pickle.loads(data)
        opts["foreground"] = True
        def unmount_callback():
            fs.close()
        opts["unmount_callback"] = unmount_callback
        mount(fs,drive,*opts)


if __name__ == "__main__":
    import os, os.path
    from fs.tempfs import TempFS
    fs = TempFS()
    fs.setcontents("test1.txt","test one")
    flags = DOKAN_OPTION_DEBUG|DOKAN_OPTION_STDERR|DOKAN_OPTION_REMOVABLE
    mount(fs, "Q", foreground=True, numthreads=1, flags=flags)


