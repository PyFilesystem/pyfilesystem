"""

  fs.expose.fuse:  expose an FS object to the native filesystem via FUSE

This module provides the necessay interfaces to mount an FS object into
the local filesystem via FUSE:

    http://fuse.sourceforge.net/

For simple usage, the function 'mount' takes an FS object and a local path,
and exposes the given FS at that path:

    >>> from fs.memoryfs import MemoryFS
    >>> from fs.expose import fuse
    >>> fs = MemoryFS()
    >>> mp = fuse.mount(fs,"/mnt/my-memory-fs")
    >>> mp.path
    '/mnt/my-memory-fs'
    >>> mp.unmount()

The above spawns a new background process to manage the FUSE event loop, which
can be controlled through the returned subprocess.Popen object.  To avoid
spawning a new process, set the 'foreground' option:

    >>> #  This will block until the filesystem is unmounted
    >>> fuse.mount(fs,"/mnt/my-memory-fs",foreground=True)

Any additional options for the FUSE process can be passed as keyword arguments
to the 'mount' function.

If you require finer control over the creation of the FUSE process, you can
instantiate the MountProcess class directly.  It accepts all options available
to subprocess.Popen:

    >>> from subprocess import PIPE
    >>> mp = fuse.MountProcess(fs,"/mnt/my-memory-fs",stderr=PIPE)
    >>> fuse_errors = mp.communicate()[1]

The binding to FUSE is created via ctypes, using a custom version of the
fuse.py code from Giorgos Verigakis:

    http://code.google.com/p/fusepy/

"""

import os
import sys
import signal
import errno
import time
import stat as statinfo
import subprocess
import pickle

from fs.base import flags_to_mode, threading
from fs.errors import *
from fs.path import *

import fuse_ctypes as fuse
try:
    fuse._libfuse.fuse_get_context
except AttributeError:
    raise ImportError("could not locate FUSE library")


FUSE = fuse.FUSE
Operations = fuse.Operations
fuse_get_context = fuse.fuse_get_context

STARTUP_TIME = time.time()
NATIVE_ENCODING = sys.getfilesystemencoding()


def handle_fs_errors(func):
    """Method decorator to report FS errors in the appropriate way.

    This decorator catches all FS errors and translates them into an
    equivalent OSError.  It also makes the function return zero instead
    of None as an indication of successful execution.
    """
    func = convert_fs_errors(func)
    @wraps(func)
    def wrapper(*args,**kwds):
        res = func(*args,**kwds)
        if res is None:
            return 0
        return res
    return wrapper
 

def get_stat_dict(fs,path):
    """Build a 'stat' dictionary for the given file."""
    uid, gid, pid = fuse_get_context()
    info = fs.getinfo(path)
    private_keys = [k for k in info if k.startswith("_")]
    for k in private_keys:
        del info[k]
    #  Basic stuff that is constant for all paths
    info.setdefault("st_ino",0)
    info.setdefault("st_dev",0)
    info.setdefault("st_uid",uid)
    info.setdefault("st_gid",gid)
    info.setdefault("st_rdev",0)
    info.setdefault("st_blksize",1024)
    info.setdefault("st_blocks",1)
    #  The interesting stuff
    info.setdefault("st_size",info.get("size",1024))
    info.setdefault("st_mode",info.get('st_mode',0700))
    if fs.isdir(path):
        info["st_mode"] = info["st_mode"] | statinfo.S_IFDIR
        info.setdefault("st_nlink",2)
    else:
        info["st_mode"] = info["st_mode"] | statinfo.S_IFREG
        info.setdefault("st_nlink",1)
    for (key1,key2) in [("st_atime","accessed_time"),("st_mtime","modified_time"),("st_ctime","created_time")]:
        if key1 not in info:
            if key2 in info:
                info[key1] = time.mktime(info[key2].timetuple())
            else:
                info[key1] = STARTUP_TIME
    return info
 

class FSOperations(Operations):
    """FUSE Operations interface delegating all activities to an FS object."""

    def __init__(self,fs,on_init=None,on_destroy=None):
        self.fs = fs
        self._on_init = on_init
        self._on_destroy = on_destroy
        self._fhmap = {}
        self._fh_lock = threading.Lock()
        self._fh_next = 1

    def _get_file(self,fh):
        try:
            return self._fhmap[fh]
        except KeyError:
            raise FSError("invalid file handle")

    def _reg_file(self,f):
        self._fh_lock.acquire()
        try:
            fh = self._fh_next
            self._fh_next += 1
            self._fhmap[fh] = f
            return fh
        finally:
            self._fh_lock.release()

    def init(self,conn):
        if self._on_init:
            self._on_init()

    def destroy(self,data):
        if self._on_destroy:
            self._on_destroy()
    
    @handle_fs_errors
    def chmod(self,path,mode):
        raise UnsupportedError("chmod")
    
    @handle_fs_errors
    def chown(self,path,uid,gid):
        raise UnsupportedError("chown")

    @handle_fs_errors
    def create(self,path,mode,fi=None):
        if fi is not None:
            raise UnsupportedError("raw_fi")
        return self._reg_file(self.fs.open(path,"w"))

    @handle_fs_errors
    def flush(self,path,fh):
        self._get_file(fh).flush()

    @handle_fs_errors
    def getattr(self,path,fh=None):
        return get_stat_dict(self.fs,path)

    @handle_fs_errors
    def getxattr(self,path,name,position=0):
        try:
            value = self.fs.getxattr(path,name)
        except AttributeError:
            raise OSError(errno.ENODATA,"no attribute '%s'" % (name,))
        else:
            if value is None:
                raise OSError(errno.ENODATA,"no attribute '%s'" % (name,))
            return value

    @handle_fs_errors
    def link(self,target,souce):
        raise UnsupportedError("link")

    @handle_fs_errors
    def listxattr(self,path):
        try:
            return self.fs.listxattrs(path)
        except AttributeError:
            return []

    @handle_fs_errors
    def mkdir(self,path,mode):
        try:
            self.fs.makedir(path,mode)
        except TypeError:
            self.fs.makedir(path)

    @handle_fs_errors
    def mknod(self,path,mode,dev):
        raise UnsupportedError("mknod")

    @handle_fs_errors
    def open(self,path,flags):
        mode = flags_to_mode(flags)
        return self._reg_file(self.fs.open(path,mode))

    @handle_fs_errors
    def read(self,path,size,offset,fh):
        f = self._get_file(fh)
        f.seek(offset)
        return f.read(size)

    @handle_fs_errors
    def readdir(self,path,fh=None):
        entries = [e.encode(NATIVE_ENCODING) for e in self.fs.listdir(path)]
        return ['.', '..'] + entries

    @handle_fs_errors
    def readlink(self,path):
        raise UnsupportedError("readlink")

    @handle_fs_errors
    def release(self,path,fh):
        self._get_file(fh).close()
        del self._fhmap[fh]

    @handle_fs_errors
    def removexattr(self,path,name):
        try:
            return self.fs.delxattr(path,name)
        except AttributeError:
            raise UnsupportedError("removexattr")

    @handle_fs_errors
    def rename(self,old,new):
        if issamedir(old,new):
            self.fs.rename(old,new)
        else:
            if self.fs.isdir(old):
                self.fs.movedir(old,new)
            else:
                self.fs.move(old,new)

    @handle_fs_errors
    def rmdir(self, path):
        self.fs.removedir(path)

    @handle_fs_errors
    def setxattr(self,path,name,value,options,position=0):
        try:
            return self.fs.setxattr(path,name,value)
        except AttributeError:
            raise UnsupportedError("setxattr")

    @handle_fs_errors
    def symlink(self, target, source):
        raise UnsupportedError("symlink")

    @handle_fs_errors
    def truncate(self, path, length, fh=None):
        if fh is None and length == 0:
            self.fs.open(path,"w").close()
        else:
            if fh is None:
                f = self.fs.open(path,"w+")
            else:
                f = self._get_file(fh)
            if not hasattr(f,"truncate"):
                raise UnsupportedError("truncate")
            f.truncate(length)

    @handle_fs_errors
    def unlink(self, path):
        self.fs.remove(path)

    @handle_fs_errors
    def utimens(self, path, times=None):
        raise UnsupportedError("utimens")

    @handle_fs_errors
    def write(self, path, data, offset, fh):
        f = self._get_file(fh)
        f.seek(offset)
        f.write(data)
        return len(data)


def mount(fs,path,foreground=False,ready_callback=None,unmount_callback=None,**kwds):
    """Mount the given FS at the given path, using FUSE.

    By default, this function spawns a new background process to manage the
    FUSE event loop.  The return value in this case is an instance of the
    'MountProcess' class, a subprocess.Popen subclass.

    If the keyword argument 'foreground' is given, we instead run the FUSE
    main loop in the current process.  In this case the function will block
    until the filesystem is unmounted, then return None.

    If the keyword argument 'ready_callback' is provided, it will be called
    when the filesystem has been mounted and is ready for use.  Any additional
    keyword arguments will be passed through as options to the underlying
    FUSE class.  Some interesting options include:

        * nothreads:  switch off threading in the FUSE event loop
        * fsname:     name to display in the mount info table

    """
    if foreground:
        op = FSOperations(fs,on_init=ready_callback,on_destroy=unmount_callback)
        return FUSE(op,path,foreground=foreground,**kwds)
    else:
        mp = MountProcess(fs,path,kwds)
        if ready_callback:
            ready_callback()
        if unmount_callback:
            orig_unmount = mp.unmount
            def new_unmount():
                orig_unmount()
                unmount_callback()
            mp.unmount = new_unmount
        return mp


def unmount(path):
    """Unmount the given mount point.

    This function shells out to the 'fusermount' program to unmount a
    FUSE filesystem.  It works, but it would probably be better to use the
    'unmount' method on the MountProcess class if you have it.
    """
    if os.system("fusermount -u '" + path + "'"):
        raise OSError("filesystem could not be unmounted: " + path)


class MountProcess(subprocess.Popen):
    """subprocess.Popen subclass managing a FUSE mount.

    This is a subclass of subprocess.Popen, designed for easy management of
    a FUSE mount in a background process.  Rather than specifying the command
    to execute, pass in the FS object to be mounted, the target mount point
    and a dictionary of options for the underlying FUSE class.

    In order to be passed successfully to the new process, the FS object
    must be pickleable.  This restriction may be lifted in the future.

    This class has an extra attribute 'path' giving the path to the mounted
    filesystem, and an extra method 'unmount' that will cleanly unmount it
    and terminate the process.

    By default, the spawning process will block until it receives notification
    that the filesystem has been mounted.  Since this notification is sent
    by writing to a pipe, using the 'close_fds' option on this class will
    prevent it from being sent.  You can also pass in the keyword argument
    'nowait' to continue without waiting for notification.

    """

    #  This works by spawning a new python interpreter and passing it the
    #  pickled (fs,path,opts) tuple on the command-line.  Something like this:
    #
    #    python -c "import MountProcess; MountProcess._do_mount('..data..')
    #
    #  It would be more efficient to do a straight os.fork() here, and would
    #  remove the need to pickle the FS.  But API wise, I think it's much
    #  better for mount() to return a Popen instance than just a pid.
    #
    #  In the future this class could implement its own forking logic and
    #  just copy the relevant bits of the Popen interface.  For now, this
    #  spawn-a-new-interpreter solution is the easiest to get up and running.

    unmount_timeout = 5

    def __init__(self,fs,path,fuse_opts={},nowait=False,**kwds):
        self.path = path
        if nowait or kwds.get("close_fds",False):
            cmd = 'from fs.expose.fuse import MountProcess; '
            cmd = cmd + 'MountProcess._do_mount_nowait(%s)'
            cmd = cmd % (pickle.dumps((fs,path,fuse_opts)),)
            cmd = cmd % (repr(pickle.dumps((fs,path,fuse_opts),-1)),)
            cmd = [sys.executable,"-c",cmd]
            super(MountProcess,self).__init__(cmd,**kwds)
        else:
            (r,w) = os.pipe()
            cmd = 'from fs.expose.fuse import MountProcess; '
            cmd = cmd + 'MountProcess._do_mount_wait(%s)'
            cmd = cmd % (repr(pickle.dumps((fs,path,fuse_opts,r,w),-1)),)
            cmd = [sys.executable,"-c",cmd]
            super(MountProcess,self).__init__(cmd,**kwds)
            os.close(w)
            if os.read(r,1) != "S":
                raise RuntimeError("A FUSE error occurred")

    def unmount(self):
        """Cleanly unmount the FUSE filesystem, terminating this subprocess."""
        self.terminate()
        tmr = threading.Timer(self.unmount_timeout,self.kill)
        tmr.start()
        self.wait()
        tmr.cancel()

    if not hasattr(subprocess.Popen,"terminate"):
        def terminate(self):
            """Gracefully terminate the subprocess."""
            os.kill(self.pid,signal.SIGTERM)

    if not hasattr(subprocess.Popen,"kill"):
        def kill(self):
            """Forcibly terminate the subprocess."""
            os.kill(self.pid,signal.SIGKILL)

    @staticmethod
    def _do_mount_nowait(data):
        """Perform the specified mount, return without waiting."""
        (fs,path,opts) = pickle.loads(data)
        opts["foreground"] = True
        if hasattr(fs,"close"):
            def unmount_callback():
                fs.close()
            opts["unmount_callback"] = unmount_callback
        mount(fs,path,*opts)

    @staticmethod
    def _do_mount_wait(data):
        """Perform the specified mount, signalling when ready."""
        (fs,path,opts,r,w) = pickle.loads(data)
        os.close(r)
        opts["foreground"] = True
        successful = []
        def ready_callback():
            successful.append(True)
            os.write(w,"S")
            os.close(w)
        opts["ready_callback"] = ready_callback
        if hasattr(fs,"close"):
            def unmount_callback():
                fs.close()
            opts["unmount_callback"] = unmount_callback
        try:
            mount(fs,path,**opts)
        except Exception:
            pass
        if not successful:
            os.write(w,"E")


if __name__ == "__main__":
    import os, os.path
    from fs.tempfs import TempFS
    mount_point = os.path.join(os.environ["HOME"],"fs.expose.fuse")
    if not os.path.exists(mount_point):
        os.makedirs(mount_point)
    def ready_callback():
        print "READY"
    mount(TempFS(),mount_point,foreground=True,ready_callback=ready_callback)

