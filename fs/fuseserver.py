#!/usr/bin/env python


import base

import fuse
fuse.fuse_python_api = (0, 2)


from datetime import datetime
import time
from os import errno

import sys
from stat import *

def showtb(f):

    def run(*args, **kwargs):
        print
        print "-"*80
        print f, args, kwargs
        try:
            ret = f(*args, **kwargs)
            print "\tReturned:", repr(ret)
            return ret
        except Exception, e:
            print e
            raise
        print "-"*80
        print
    return run

"""
::*'''<code>open(path, flags)</code>'''

::*'''<code>create(path, flags, mode)</code>'''

::*'''<code>read(path, length, offset, fh=None)</code>'''

::*'''<code>write(path, buf, offset, fh=None)</code>'''

::*'''<code>fgetattr(path, fh=None)</code>'''

::*'''<code>ftruncate(path, len, fh=None)</code>'''

::*'''<code>flush(path, fh=None)</code>'''

::*'''<code>release(path, fh=None)</code>'''

::*'''<code>fsync(path, fdatasync, fh=None)</code>'''

"""


class FuseFile(object):

    def __init__(self, f):
        self.f = f





_run_t = time.time()
class FSFUSE(fuse.Fuse):

    def __init__(self, fs, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)
        self._fs = fs

    @showtb
    def fsinit(self):
        return 0

    def __getattr__(self, name):
        print name
        raise AttributeError

    #@showtb
    def getattr(self, path):

        if not self._fs.exists(path):
            return -errno.ENOENT

        class Stat(fuse.Stat):
            def __init__(self, context, fs, path):
                fuse.Stat.__init__(self)
                info = fs.getinfo(path)
                isdir = fs.isdir(path)

                fsize = fs.getsize(path) or 1024
                self.st_ino = 0
                self.st_dev = 0
                self.st_nlink = 2 if isdir else 1
                self.st_blksize = fsize
                self.st_mode = info.get('st_mode', S_IFDIR | 0755 if isdir else S_IFREG | 0666)
                print self.st_mode
                self.st_uid = context['uid']
                self.st_gid = context['gid']
                self.st_rdev = 0
                self.st_size = fsize
                self.st_blocks = 1

                for key, value in info.iteritems():
                    if not key.startswith('_'):
                        setattr(self, key, value)

                def do_time(attr, key):
                    if not hasattr(self, attr):
                        if key in info:
                            info_t = info[key]
                            setattr(self, attr, time.mktime(info_t.timetuple()))
                        else:
                            setattr(self, attr, _run_t)

                do_time('st_atime', 'accessed_time')
                do_time('st_mtime', 'modified_time')
                do_time('st_ctime', 'created_time')

                #for v in dir(self):
                #    if not v.startswith('_'):
                #        print v, getattr(self, v)

        return Stat(self.GetContext(), self._fs, path)

    @showtb
    def chmod(self, path, mode):
        return 0

    @showtb
    def chown(self, path, user, group):
        return 0

    @showtb
    def utime(self, path, times):
        return 0

    @showtb
    def utimens(self, path, times):
        return 0

    @showtb
    def fsyncdir(self):
        pass

    @showtb
    def bmap(self):
        return 0

    @showtb
    def ftruncate(self, path, flags, fh):
        if fh is not None:
            fh.truncate()
            fh.flush()
        return 0

    def fsdestroy(self):
        return 0

    @showtb
    def statfs(self):
        return (0, 0, 0, 0, 0, 0, 0)



    #def setattr
    #
    #
    #@showtb
    #def getdir(self, path, offset):
    #    paths = ['.', '..']
    #    paths += self._fs.listdir(path)
    #    print repr(paths)
    #
    #    for p in paths:
    #        yield fuse.Direntry(p)

    @showtb
    def opendir(self, path):
        return 0

    @showtb
    def getxattr(self, path, name, default):
        return self._fs.getattr(path, name, default)

    @showtb
    def setxattr(self, path, name, value):
        self._fs.setattr(path, name)
        return 0

    @showtb
    def removeattr(self, path, name):
        self._fs.removeattr(path, name)
        return 0

    @showtb
    def listxattr(self, path, something):
        return self._fs.listattrs(path)

    @showtb
    def open(self, path, flags):
        return self._fs.open(path, flags=flags)

    @showtb
    def create(self, path, flags, mode):
        return self._fs.open(path, "w")

    @showtb
    def read(self, path, length, offset, fh=None):
        if fh:
            fh.seek(offset)
            return fh.read(length)

    @showtb
    def write(self, path, buf, offset, fh=None):
        if fh:
            fh.seek(offset)
            # FUSE seems to expect a return value of the number of bytes written,
            # but Python file objects don't return that information,
            # so we will assume all bytes are written...
            bytes_written = fh.write(buf) or len(buf)
            return bytes_written

    @showtb
    def release(self, path, flags, fh=None):
        if fh:
            fh.close()
            return 0

    @showtb
    def flush(self, path, fh=None):
        if fh:
            try:
                fh.flush()
            except base.FSError:
                return 0
            return 0

    @showtb
    def access(self, path, *args, **kwargs):
        return 0


    #@showtb
    def readdir(self, path, offset):
        paths = ['.', '..']
        paths += self._fs.listdir(path)
        return [fuse.Direntry(p) for p in paths]

    #@showtb
    #def fgetattr(self, path, fh=None):
    #    fh.flush()
    #    return self.getattr(path)

    @showtb
    def readlink(self, path):
        return path

    @showtb
    def symlink(self, path, path1):
        return 0


    @showtb
    def mknod(self, path, mode, rdev):
        f = None
        try:
            f = self._fs.open(path, mode)
        finally:
            f.close()
        return 0

    @showtb
    def mkdir(self, path, mode):
        self._fs.mkdir(path, mode)
        return 0

    @showtb
    def rmdir(self, path):
        self._fs.removedir(path, True)
        return 0

    @showtb
    def unlink(self, path):
        try:
            self._fs.remove(path)
        except base.FSError:
            return 0
        return 0

    #symlink(target, name)

    @showtb
    def rename(self, old, new):
        self._fs.rename(old, new)
        return 0



    #@showtb
    #def read(self, path, size, offset):
    #    pass



def main(fs):
    usage="""
        FSFS: Exposes an FS
    """ + fuse.Fuse.fusage

    server = FSFUSE(fs, version="%prog 0.1",
                    usage=usage, dash_s_do='setsingle')

    #server.readdir('.', 0)

    server.parse(errex=1)
    server.main()


if __name__ == "__main__":

    import memoryfs
    import osfs
    mem_fs = memoryfs.MemoryFS()
    mem_fs.makedir("test")
    mem_fs.createfile("a.txt", "This is a test")
    mem_fs.createfile("test/b.txt", "This is in a sub-dir")


    #fs = osfs.OSFS('/home/will/fusetest/')
    #main(fs)

    main(mem_fs)
