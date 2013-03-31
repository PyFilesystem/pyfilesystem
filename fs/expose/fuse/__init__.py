"""
fs.expose.fuse
==============

Expose an FS object to the native filesystem via FUSE

This module provides the necessary interfaces to mount an FS object into
the local filesystem via FUSE::

    http://fuse.sourceforge.net/

For simple usage, the function 'mount' takes an FS object and a local path,
and exposes the given FS at that path::

    >>> from fs.memoryfs import MemoryFS
    >>> from fs.expose import fuse
    >>> fs = MemoryFS()
    >>> mp = fuse.mount(fs,"/mnt/my-memory-fs")
    >>> mp.path
    '/mnt/my-memory-fs'
    >>> mp.unmount()

The above spawns a new background process to manage the FUSE event loop, which
can be controlled through the returned subprocess.Popen object.  To avoid
spawning a new process, set the 'foreground' option::

    >>> #  This will block until the filesystem is unmounted
    >>> fuse.mount(fs,"/mnt/my-memory-fs",foreground=True)

Any additional options for the FUSE process can be passed as keyword arguments
to the 'mount' function.

If you require finer control over the creation of the FUSE process, you can
instantiate the MountProcess class directly.  It accepts all options available
to subprocess.Popen::

    >>> from subprocess import PIPE
    >>> mp = fuse.MountProcess(fs,"/mnt/my-memory-fs",stderr=PIPE)
    >>> fuse_errors = mp.communicate()[1]

The binding to FUSE is created via ctypes, using a custom version of the
fuse.py code from Giorgos Verigakis:

    http://code.google.com/p/fusepy/

"""

import sys
if sys.platform == "win32":
    raise ImportError("FUSE is not available on win32")

import datetime
import os
import signal
import errno
import time
import stat as statinfo
import subprocess
import cPickle

import logging
logger = logging.getLogger("fs.expose.fuse")

from fs.base import flags_to_mode, threading
from fs.errors import *
from fs.path import *
from fs.local_functools import wraps

from six import PY3
from six import b

try:
    if PY3:
        from fs.expose.fuse import fuse_ctypes as fuse
    else:
        from fs.expose.fuse import fuse3 as fuse

except NotImplementedError:
    raise ImportError("FUSE found but not usable")
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
    name = func.__name__
    func = convert_fs_errors(func)
    @wraps(func)
    def wrapper(*args,**kwds):
        #logger.debug("CALL %r %s",name,repr(args))
        try:
            res = func(*args,**kwds)
        except Exception:
            #logger.exception("ERR %r %s",name,repr(args))
            raise
        else:
            #logger.exception("OK %r %s %r",name,repr(args),res)
            if res is None:
                return 0
        return res
    return wrapper


class FSOperations(Operations):
    """FUSE Operations interface delegating all activities to an FS object."""

    def __init__(self, fs, on_init=None, on_destroy=None):
        self.fs = fs
        self._on_init = on_init
        self._on_destroy = on_destroy
        self._files_by_handle = {}
        self._files_lock = threading.Lock()
        self._next_handle = 1
        #  FUSE expects a succesful write() to be reflected in the file's
        #  reported size, but the FS might buffer writes and prevent this.
        #  We explicitly keep track of the size FUSE expects a file to be.
        #  This dict is indexed by path, then file handle.
        self._files_size_written = {}

    def _get_file(self, fh):
        try:
            return self._files_by_handle[fh.fh]
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
            (f,path,lock) = self._files_by_handle.pop(fh.fh)
            del self._files_size_written[path][fh.fh]
            if not self._files_size_written[path]:
                del self._files_size_written[path]
        finally:
            self._files_lock.release()

    def init(self, conn):
        if self._on_init:
            self._on_init()

    def destroy(self, data):
        if self._on_destroy:
            self._on_destroy()

    @handle_fs_errors
    def chmod(self, path, mode):
        raise UnsupportedError("chmod")

    @handle_fs_errors
    def chown(self, path, uid, gid):
        raise UnsupportedError("chown")

    @handle_fs_errors
    def create(self, path, mode, fi):
        path = path.decode(NATIVE_ENCODING)
        # FUSE doesn't seem to pass correct mode information here - at least,
        # I haven't figured out how to distinguish between "w" and "w+".
        # Go with the most permissive option.
        mode = flags_to_mode(fi.flags)
        fh = self._reg_file(self.fs.open(path, mode), path)
        fi.fh = fh
        fi.keep_cache = 0

    @handle_fs_errors
    def flush(self, path, fh):
        (file, _, lock) = self._get_file(fh)
        lock.acquire()
        try:
            file.flush()
        finally:
            lock.release()

    @handle_fs_errors
    def getattr(self, path, fh=None):
        attrs = self._get_stat_dict(path.decode(NATIVE_ENCODING))
        return attrs

    @handle_fs_errors
    def getxattr(self, path, name, position=0):
        path = path.decode(NATIVE_ENCODING)
        name = name.decode(NATIVE_ENCODING)
        try:
            value = self.fs.getxattr(path, name)
        except AttributeError:
            raise UnsupportedError("getxattr")
        else:
            if value is None:
                raise OSError(errno.ENODATA, "no attribute '%s'" % (name,))
            return value

    @handle_fs_errors
    def link(self, target, souce):
        raise UnsupportedError("link")

    @handle_fs_errors
    def listxattr(self, path):
        path = path.decode(NATIVE_ENCODING)
        try:
            return self.fs.listxattrs(path)
        except AttributeError:
            raise UnsupportedError("listxattrs")

    @handle_fs_errors
    def mkdir(self, path, mode):
        path = path.decode(NATIVE_ENCODING)
        try:
            self.fs.makedir(path, recursive=True)
        except TypeError:
            self.fs.makedir(path)

    @handle_fs_errors
    def mknod(self, path, mode, dev):
        raise UnsupportedError("mknod")

    @handle_fs_errors
    def open(self, path, fi):
        path = path.decode(NATIVE_ENCODING)
        mode = flags_to_mode(fi.flags)
        fi.fh = self._reg_file(self.fs.open(path, mode), path)
        fi.keep_cache = 0
        return 0

    @handle_fs_errors
    def read(self, path, size, offset, fh):
        (file, _, lock) = self._get_file(fh)
        lock.acquire()
        try:
            file.seek(offset)
            data = file.read(size)
            return data
        finally:
            lock.release()

    @handle_fs_errors
    def readdir(self, path, fh=None):
        path = path.decode(NATIVE_ENCODING)
        entries = ['.', '..']
        for (nm, info) in self.fs.listdirinfo(path):
            self._fill_stat_dict(pathjoin(path, nm), info)
            entries.append((nm.encode(NATIVE_ENCODING), info, 0))
        return entries

    @handle_fs_errors
    def readlink(self, path):
        raise UnsupportedError("readlink")

    @handle_fs_errors
    def release(self, path, fh):
        (file, _, lock) = self._get_file(fh)
        lock.acquire()
        try:
            file.close()
            self._del_file(fh)
        finally:
            lock.release()

    @handle_fs_errors
    def removexattr(self, path, name):
        path = path.decode(NATIVE_ENCODING)
        name = name.decode(NATIVE_ENCODING)
        try:
            return self.fs.delxattr(path, name)
        except AttributeError:
            raise UnsupportedError("removexattr")

    @handle_fs_errors
    def rename(self, old, new):
        old = old.decode(NATIVE_ENCODING)
        new = new.decode(NATIVE_ENCODING)
        try:
            self.fs.rename(old, new)
        except FSError:
            if self.fs.isdir(old):
                self.fs.movedir(old, new)
            else:
                self.fs.move(old, new)

    @handle_fs_errors
    def rmdir(self, path):
        path = path.decode(NATIVE_ENCODING)
        self.fs.removedir(path)

    @handle_fs_errors
    def setxattr(self, path, name, value, options, position=0):
        path = path.decode(NATIVE_ENCODING)
        name = name.decode(NATIVE_ENCODING)
        try:
            return self.fs.setxattr(path, name, value)
        except AttributeError:
            raise UnsupportedError("setxattr")

    @handle_fs_errors
    def symlink(self, target, source):
        raise UnsupportedError("symlink")

    @handle_fs_errors
    def truncate(self, path, length, fh=None):
        path = path.decode(NATIVE_ENCODING)
        if fh is None and length == 0:
            self.fs.open(path, "wb").close()
        else:
            if fh is None:
                f = self.fs.open(path, "rb+")
                if not hasattr(f, "truncate"):
                    raise UnsupportedError("truncate")
                f.truncate(length)
            else:
                (file, _, lock) = self._get_file(fh)
                lock.acquire()
                try:
                    if not hasattr(file, "truncate"):
                        raise UnsupportedError("truncate")
                    file.truncate(length)
                finally:
                    lock.release()
        self._files_lock.acquire()
        try:
            try:
                size_written = self._files_size_written[path]
            except KeyError:
                pass
            else:
                for k in size_written:
                    size_written[k] = length
        finally:
            self._files_lock.release()

    @handle_fs_errors
    def unlink(self, path):
        path = path.decode(NATIVE_ENCODING)
        self.fs.remove(path)

    @handle_fs_errors
    def utimens(self, path, times=None):
        path = path.decode(NATIVE_ENCODING)
        accessed_time, modified_time = times
        if accessed_time is not None:
            accessed_time = datetime.datetime.fromtimestamp(accessed_time)
        if modified_time is not None:
            modified_time = datetime.datetime.fromtimestamp(modified_time)
        self.fs.settimes(path, accessed_time, modified_time)

    @handle_fs_errors
    def write(self, path, data, offset, fh):
        (file, path, lock) = self._get_file(fh)
        lock.acquire()
        try:
            file.seek(offset)
            file.write(data)
            if self._files_size_written[path][fh.fh] < offset + len(data):
                self._files_size_written[path][fh.fh] = offset + len(data)
            return len(data)
        finally:
            lock.release()

    def _get_stat_dict(self, path):
        """Build a 'stat' dictionary for the given file."""
        info = self.fs.getinfo(path)
        self._fill_stat_dict(path, info)
        return info

    def _fill_stat_dict(self, path, info):
        """Fill default values in the stat dict."""
        uid, gid, pid = fuse_get_context()
        private_keys = [k for k in info if k.startswith("_")]
        for k in private_keys:
            del info[k]
        #  Basic stuff that is constant for all paths
        info.setdefault("st_ino", 0)
        info.setdefault("st_dev", 0)
        info.setdefault("st_uid", uid)
        info.setdefault("st_gid", gid)
        info.setdefault("st_rdev", 0)
        info.setdefault("st_blksize", 1024)
        info.setdefault("st_blocks", 1)
        #  The interesting stuff
        if 'st_mode' not in info:
            if self.fs.isdir(path):
                info['st_mode'] = 0755
            else:
                info['st_mode'] = 0666
        mode = info['st_mode']
        if not statinfo.S_ISDIR(mode) and not statinfo.S_ISREG(mode):
            if self.fs.isdir(path):
                info["st_mode"] = mode | statinfo.S_IFDIR
                info.setdefault("st_nlink", 2)
            else:
                info["st_mode"] = mode | statinfo.S_IFREG
                info.setdefault("st_nlink", 1)
        for (key1, key2) in [("st_atime", "accessed_time"), ("st_mtime", "modified_time"), ("st_ctime", "created_time")]:
            if key1 not in info:
                if key2 in info:
                    info[key1] = time.mktime(info[key2].timetuple())
                else:
                    info[key1] = STARTUP_TIME
        #  Ensure the reported size reflects any writes performed, even if
        #  they haven't been flushed to the filesystem yet.
        if "size" in info:
            info["st_size"] = info["size"]
        elif "st_size" not in info:
            info["st_size"] = 0
        try:
            written_sizes = self._files_size_written[path]
        except KeyError:
            pass
        else:
            info["st_size"] = max(written_sizes.values() + [info["st_size"]])
        return info


def mount(fs, path, foreground=False, ready_callback=None, unmount_callback=None, **kwds):
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

        * nothreads Switch off threading in the FUSE event loop
        * fsname Name to display in the mount info table

    """
    path = os.path.expanduser(path)
    if foreground:
        op = FSOperations(fs, on_init=ready_callback, on_destroy=unmount_callback)
        return FUSE(op, path, raw_fi=True, foreground=foreground, **kwds)
    else:
        mp = MountProcess(fs, path, kwds)
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

    On darwin, "diskutil umount <path>" is called
    On freebsd, "umount <path>" is called
    """
    if sys.platform == "darwin":
        args = ["diskutil", "umount", path]
    elif "freebsd" in sys.platform:
        args = ["umount", path]
    else:
        args = ["fusermount", "-u", path]

    for num_tries in xrange(3):
        p = subprocess.Popen(args,
                             stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        if p.returncode == 0:
            return
        if "not mounted" in stderr:
            return
        if "not found" in stderr:
            return
    raise OSError("filesystem could not be unmounted: %s (%s) " % (path, str(stderr).rstrip(),))


class MountProcess(subprocess.Popen):
    """subprocess.Popen subclass managing a FUSE mount.

    This is a subclass of subprocess.Popen, designed for easy management of
    a FUSE mount in a background process.  Rather than specifying the command
    to execute, pass in the FS object to be mounted, the target mount point
    and a dictionary of options for the underlying FUSE class.

    In order to be passed successfully to the new process, the FS object
    must be pickleable. This restriction may be lifted in the future.

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

    def __init__(self, fs, path, fuse_opts={}, nowait=False, **kwds):
        self.path = path
        if nowait or kwds.get("close_fds", False):
            if PY3:
                cmd = "from pickle import loads;"
            else:
                cmd = "from cPickle import loads;"
            #cmd = 'import cPickle; '
            cmd = cmd + 'data = loads(%s); '
            cmd = cmd + 'from fs.expose.fuse import MountProcess; '
            cmd = cmd + 'MountProcess._do_mount_nowait(data)'
            cmd = cmd % (repr(cPickle.dumps((fs, path, fuse_opts), -1)),)
            cmd = [sys.executable, "-c", cmd]
            super(MountProcess, self).__init__(cmd, **kwds)
        else:
            (r, w) = os.pipe()
            if PY3:
                cmd = "from pickle import loads;"
            else:
                cmd = "from cPickle import loads;"
            #cmd = 'import cPickle; '
            cmd = cmd + 'data = loads(%s); '
            cmd = cmd + 'from fs.expose.fuse import MountProcess; '
            cmd = cmd + 'MountProcess._do_mount_wait(data)'
            cmd = cmd % (repr(cPickle.dumps((fs, path, fuse_opts, r, w), -1)),)
            cmd = [sys.executable, "-c", cmd]
            super(MountProcess, self).__init__(cmd, **kwds)
            os.close(w)

            byte = os.read(r, 1)
            if byte != b("S"):
                err_text = os.read(r, 20)
                self.terminate()
                if hasattr(err_text, 'decode'):
                    err_text = err_text.decode(NATIVE_ENCODING)
                raise RuntimeError("FUSE error: " + err_text)

    def unmount(self):
        """Cleanly unmount the FUSE filesystem, terminating this subprocess."""
        self.terminate()
        def killme():
            self.kill()
            time.sleep(0.1)
            try:
                unmount(self.path)
            except OSError:
                pass
        tmr = threading.Timer(self.unmount_timeout, killme)
        tmr.start()
        self.wait()
        tmr.cancel()

    if not hasattr(subprocess.Popen, "terminate"):
        def terminate(self):
            """Gracefully terminate the subprocess."""
            os.kill(self.pid, signal.SIGTERM)

    if not hasattr(subprocess.Popen, "kill"):
        def kill(self):
            """Forcibly terminate the subprocess."""
            os.kill(self.pid, signal.SIGKILL)

    @staticmethod
    def _do_mount_nowait(data):
        """Perform the specified mount, return without waiting."""
        fs, path, opts = data
        opts["foreground"] = True

        def unmount_callback():
            fs.close()
        opts["unmount_callback"] = unmount_callback
        mount(fs, path, *opts)

    @staticmethod
    def _do_mount_wait(data):
        """Perform the specified mount, signalling when ready."""
        fs, path, opts, r, w = data
        os.close(r)
        opts["foreground"] = True
        successful = []

        def ready_callback():
            successful.append(True)
            os.write(w, b("S"))
            os.close(w)
        opts["ready_callback"] = ready_callback

        def unmount_callback():
            fs.close()
        opts["unmount_callback"] = unmount_callback
        try:
            mount(fs, path, **opts)
        except Exception, e:
            os.write(w, b("E") + unicode(e).encode('ascii', errors='replace'))
            os.close(w)

        if not successful:
            os.write(w, b("EMount unsuccessful"))
            os.close(w)


if __name__ == "__main__":
    import os
    import os.path
    from fs.tempfs import TempFS
    mount_point = os.path.join(os.environ["HOME"], "fs.expose.fuse")
    if not os.path.exists(mount_point):
        os.makedirs(mount_point)

    def ready_callback():
        print "READY"
    mount(TempFS(), mount_point, foreground=True, ready_callback=ready_callback)
