"""
fs.wrapfs
=========

A class for wrapping an existing FS object with additional functionality.

This module provides the class WrapFS, a base class for objects that wrap
another FS object and provide some transformation of its contents.  It could
be very useful for implementing e.g. transparent encryption or compression
services.

For a simple example of how this class could be used, see the 'HideDotFilesFS'
class in the module fs.wrapfs.hidedotfilesfs.  This wrapper implements the
standard unix shell functionality of hiding dot-files in directory listings.

"""

import re
import sys
import fnmatch
import threading

from fs.base import FS, threading, synchronize, NoDefaultMeta
from fs.errors import *
from fs.path import *
from fs.local_functools import wraps


def rewrite_errors(func):
    """Re-write paths in errors raised by wrapped FS objects."""
    @wraps(func)
    def wrapper(self,*args,**kwds):
        try:
            return func(self,*args,**kwds)
        except ResourceError, e:
            (exc_type,exc_inst,tb) = sys.exc_info()
            try:
                e.path = self._decode(e.path)
            except (AttributeError, ValueError, TypeError):
                raise e, None, tb
            raise
    return wrapper


class WrapFS(FS):
    """FS that wraps another FS, providing translation etc.

    This class allows simple transforms to be applied to the names
    and/or contents of files in an FS.  It could be used to implement
    e.g. compression or encryption in a relatively painless manner.

    The following methods can be overridden to control how files are
    accessed in the underlying FS object:

     * _file_wrap(file, mode):  called for each file that is opened from
                                the underlying FS; may return a modified
                                file-like object.

     *  _encode(path):  encode a path for access in the underlying FS

     *  _decode(path):  decode a path from the underlying FS

    If the required path translation proceeds one component at a time,
    it may be simpler to override the _encode_name() and _decode_name()
    methods.
    """

    def __init__(self, fs):
        super(WrapFS, self).__init__()
        try:
            self._lock = fs._lock
        except (AttributeError,FSError):
            self._lock = self._lock = threading.RLock()
        self.wrapped_fs = fs

    def _file_wrap(self, f, mode):
        """Apply wrapping to an opened file."""
        return f

    def _encode_name(self, name):
        """Encode path component for the underlying FS."""
        return name

    def _decode_name(self, name):
        """Decode path component from the underlying FS."""
        return name

    def _encode(self, path):
        """Encode path for the underlying FS."""
        e_names = []
        for name in iteratepath(path):
            if name == "":
                e_names.append("")
            else:
                e_names.append(self._encode_name(name))
        return "/".join(e_names)

    def _decode(self, path):
        """Decode path from the underlying FS."""
        d_names = []
        for name in iteratepath(path):
            if name == "":
                d_names.append("")
            else:
                d_names.append(self._decode_name(name))
        return "/".join(d_names)

    def _adjust_mode(self, mode):
        """Adjust the mode used to open a file in the underlying FS.

        This method takes the mode given when opening a file, and should
        return a two-tuple giving the mode to be used in this FS as first
        item, and the mode to be used in the underlying FS as the second.

        An example of why this is needed is a WrapFS subclass that does
        transparent file compression - in this case files from the wrapped
        FS cannot be opened in append mode.
        """
        return (mode, mode)

    def __unicode__(self):
        return u"<%s: %s>" % (self.__class__.__name__,self.wrapped_fs,)

    #def __str__(self):
    #    return unicode(self).encode(sys.getdefaultencoding(),"replace")


    @rewrite_errors
    def getmeta(self, meta_name, default=NoDefaultMeta):
        return self.wrapped_fs.getmeta(meta_name, default)

    @rewrite_errors
    def hasmeta(self, meta_name):
        return self.wrapped_fs.hasmeta(meta_name)

    @rewrite_errors
    def validatepath(self, path):
        return self.wrapped_fs.validatepath(self._encode(path))

    @rewrite_errors
    def getsyspath(self, path, allow_none=False):
        return self.wrapped_fs.getsyspath(self._encode(path), allow_none)

    @rewrite_errors
    def getpathurl(self, path, allow_none=False):
        return self.wrapped_fs.getpathurl(self._encode(path), allow_none)

    @rewrite_errors
    def hassyspath(self, path):
        return self.wrapped_fs.hassyspath(self._encode(path))

    @rewrite_errors
    def open(self, path, mode='r', **kwargs):
        (mode, wmode) = self._adjust_mode(mode)
        f = self.wrapped_fs.open(self._encode(path), wmode, **kwargs)
        return self._file_wrap(f, mode)

    @rewrite_errors
    def setcontents(self, path, data, encoding=None, errors=None, chunk_size=64*1024):
        #  We can't pass setcontents() through to the wrapped FS if the
        #  wrapper has defined a _file_wrap method, as it would bypass
        #  the file contents wrapping.
        #if self._file_wrap.im_func is WrapFS._file_wrap.im_func:
        if getattr(self.__class__, '_file_wrap', None) is getattr(WrapFS, '_file_wrap', None):
            return self.wrapped_fs.setcontents(self._encode(path), data, encoding=encoding, errors=errors, chunk_size=chunk_size)
        else:
            return super(WrapFS, self).setcontents(path, data, encoding=encoding, errors=errors, chunk_size=chunk_size)

    @rewrite_errors
    def createfile(self, path, wipe=False):
        return self.wrapped_fs.createfile(self._encode(path), wipe=wipe)

    @rewrite_errors
    def exists(self, path):
        return self.wrapped_fs.exists(self._encode(path))

    @rewrite_errors
    def isdir(self, path):
        return self.wrapped_fs.isdir(self._encode(path))

    @rewrite_errors
    def isfile(self, path):
        return self.wrapped_fs.isfile(self._encode(path))

    @rewrite_errors
    def listdir(self, path="", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        full = kwds.pop("full",False)
        absolute = kwds.pop("absolute",False)
        wildcard = kwds.pop("wildcard",None)
        if wildcard is None:
            wildcard = lambda fn:True
        elif not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn:bool (wildcard_re.match(fn))
        entries = []
        enc_path = self._encode(path)
        for e in self.wrapped_fs.listdir(enc_path,**kwds):
            e = basename(self._decode(pathcombine(enc_path,e)))
            if not wildcard(e):
                continue
            if full:
                e = pathcombine(path,e)
            elif absolute:
                e = abspath(pathcombine(path,e))
            entries.append(e)
        return entries

    @rewrite_errors
    def ilistdir(self, path="", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        full = kwds.pop("full",False)
        absolute = kwds.pop("absolute",False)
        wildcard = kwds.pop("wildcard",None)
        if wildcard is None:
            wildcard = lambda fn:True
        elif not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn:bool (wildcard_re.match(fn))
        enc_path = self._encode(path)
        for e in self.wrapped_fs.ilistdir(enc_path,**kwds):
            e = basename(self._decode(pathcombine(enc_path,e)))
            if not wildcard(e):
                continue
            if full:
                e = pathcombine(path,e)
            elif absolute:
                e = abspath(pathcombine(path,e))
            yield e

    @rewrite_errors
    def listdirinfo(self, path="", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        full = kwds.pop("full",False)
        absolute = kwds.pop("absolute",False)
        wildcard = kwds.pop("wildcard",None)
        if wildcard is None:
            wildcard = lambda fn:True
        elif not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn:bool (wildcard_re.match(fn))
        entries = []
        enc_path = self._encode(path)
        for (nm,info) in self.wrapped_fs.listdirinfo(enc_path,**kwds):
            nm = basename(self._decode(pathcombine(enc_path,nm)))
            if not wildcard(nm):
                continue
            if full:
                nm = pathcombine(path,nm)
            elif absolute:
                nm = abspath(pathcombine(path,nm))
            entries.append((nm,info))
        return entries

    @rewrite_errors
    def ilistdirinfo(self, path="", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        kwds = dict(wildcard=wildcard,
                    full=full,
                    absolute=absolute,
                    dirs_only=dirs_only,
                    files_only=files_only)
        full = kwds.pop("full",False)
        absolute = kwds.pop("absolute",False)
        wildcard = kwds.pop("wildcard",None)
        if wildcard is None:
            wildcard = lambda fn:True
        elif not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn:bool (wildcard_re.match(fn))
        enc_path = self._encode(path)
        for (nm,info) in self.wrapped_fs.ilistdirinfo(enc_path,**kwds):
            nm = basename(self._decode(pathcombine(enc_path,nm)))
            if not wildcard(nm):
                continue
            if full:
                nm = pathcombine(path,nm)
            elif absolute:
                nm = abspath(pathcombine(path,nm))
            yield (nm,info)

    @rewrite_errors
    def walk(self,path="/",wildcard=None,dir_wildcard=None,search="breadth",ignore_errors=False):
        if dir_wildcard is not None:
            #  If there is a dir_wildcard, fall back to the default impl
            #  that uses listdir().  Otherwise we run the risk of enumerating
            #  lots of directories that will just be thrown away.
            for item in super(WrapFS,self).walk(path,wildcard,dir_wildcard,search,ignore_errors):
                yield item
        #  Otherwise, the wrapped FS may provide a more efficient impl
        #  which we can use directly.
        else:
            if wildcard is not None and not callable(wildcard):
                wildcard_re = re.compile(fnmatch.translate(wildcard))
                wildcard = lambda fn:bool (wildcard_re.match(fn))
            for (dirpath,filepaths) in self.wrapped_fs.walk(self._encode(path),search=search,ignore_errors=ignore_errors):
                filepaths = [basename(self._decode(pathcombine(dirpath,p)))
                                 for p in filepaths]
                dirpath = abspath(self._decode(dirpath))
                if wildcard is not None:
                    filepaths = [p for p in filepaths if wildcard(p)]
                yield (dirpath,filepaths)

    @rewrite_errors
    def walkfiles(self,path="/",wildcard=None,dir_wildcard=None,search="breadth",ignore_errors=False):
        if dir_wildcard is not None:
            #  If there is a dir_wildcard, fall back to the default impl
            #  that uses listdir().  Otherwise we run the risk of enumerating
            #  lots of directories that will just be thrown away.
            for item in super(WrapFS,self).walkfiles(path,wildcard,dir_wildcard,search,ignore_errors):
                yield item
        #  Otherwise, the wrapped FS may provide a more efficient impl
        #  which we can use directly.
        else:
            if wildcard is not None and not callable(wildcard):
                wildcard_re = re.compile(fnmatch.translate(wildcard))
                wildcard = lambda fn:bool (wildcard_re.match(fn))
            for filepath in self.wrapped_fs.walkfiles(self._encode(path),search=search,ignore_errors=ignore_errors):
                filepath = abspath(self._decode(filepath))
                if wildcard is not None:
                    if not wildcard(basename(filepath)):
                        continue
                yield filepath

    @rewrite_errors
    def walkdirs(self,path="/",wildcard=None,search="breadth",ignore_errors=False):
        if wildcard is not None:
            #  If there is a wildcard, fall back to the default impl
            #  that uses listdir().  Otherwise we run the risk of enumerating
            #  lots of directories that will just be thrown away.
            for item in super(WrapFS,self).walkdirs(path,wildcard,search,ignore_errors):
                yield item
        #  Otherwise, the wrapped FS may provide a more efficient impl
        #  which we can use directly.
        else:
            for dirpath in self.wrapped_fs.walkdirs(self._encode(path),search=search,ignore_errors=ignore_errors):
                yield abspath(self._decode(dirpath))


    @rewrite_errors
    def makedir(self, path, *args, **kwds):
        return self.wrapped_fs.makedir(self._encode(path),*args,**kwds)

    @rewrite_errors
    def remove(self, path):
        return self.wrapped_fs.remove(self._encode(path))

    @rewrite_errors
    def removedir(self, path, *args, **kwds):
        return self.wrapped_fs.removedir(self._encode(path),*args,**kwds)

    @rewrite_errors
    def rename(self, src, dst):
        return self.wrapped_fs.rename(self._encode(src),self._encode(dst))

    @rewrite_errors
    def getinfo(self, path):
        return self.wrapped_fs.getinfo(self._encode(path))

    @rewrite_errors
    def settimes(self, path, *args, **kwds):
        return self.wrapped_fs.settimes(self._encode(path), *args,**kwds)

    @rewrite_errors
    def desc(self, path):
        return self.wrapped_fs.desc(self._encode(path))

    @rewrite_errors
    def copy(self, src, dst, **kwds):
        return self.wrapped_fs.copy(self._encode(src),self._encode(dst),**kwds)

    @rewrite_errors
    def move(self, src, dst, **kwds):
        return self.wrapped_fs.move(self._encode(src),self._encode(dst),**kwds)

    @rewrite_errors
    def movedir(self, src, dst, **kwds):
        return self.wrapped_fs.movedir(self._encode(src),self._encode(dst),**kwds)

    @rewrite_errors
    def copydir(self, src, dst, **kwds):
        return self.wrapped_fs.copydir(self._encode(src),self._encode(dst),**kwds)

    @rewrite_errors
    def getxattr(self, path, name, default=None):
        try:
            return self.wrapped_fs.getxattr(self._encode(path),name,default)
        except AttributeError:
            raise UnsupportedError("getxattr")

    @rewrite_errors
    def setxattr(self, path, name, value):
        try:
            return self.wrapped_fs.setxattr(self._encode(path),name,value)
        except AttributeError:
            raise UnsupportedError("setxattr")

    @rewrite_errors
    def delxattr(self, path, name):
        try:
            return self.wrapped_fs.delxattr(self._encode(path),name)
        except AttributeError:
            raise UnsupportedError("delxattr")

    @rewrite_errors
    def listxattrs(self, path):
        try:
            return self.wrapped_fs.listxattrs(self._encode(path))
        except AttributeError:
            raise UnsupportedError("listxattrs")

    def __getattr__(self, attr):
        #  These attributes can be used by the destructor, but may not be
        #  defined if there are errors in the constructor.
        if attr == "closed":
            return False
        if attr == "wrapped_fs":
            return None
        if attr.startswith("_"):
            raise AttributeError(attr)
        return getattr(self.wrapped_fs,attr)

    @rewrite_errors
    def close(self):
        if not self.closed:
            self.wrapped_fs.close()
            super(WrapFS,self).close()
            self.wrapped_fs = None


def wrap_fs_methods(decorator, cls=None, exclude=[]):
    """Apply the given decorator to all FS methods on the given class.

    This function can be used in two ways.  When called with two arguments it
    applies the given function 'decorator' to each FS method of the given
    class.  When called with just a single argument, it creates and returns
    a class decorator which will do the same thing when applied.  So you can
    use it like this::

        wrap_fs_methods(mydecorator,MyFSClass)

    Or on more recent Python versions, like this::

        @wrap_fs_methods(mydecorator)
        class MyFSClass(FS):
            ...

    """
    def apply_decorator(cls):
        for method_name in wrap_fs_methods.method_names:
            if method_name in exclude:
                continue
            method = getattr(cls,method_name,None)
            if method is not None:
                setattr(cls,method_name,decorator(method))
        return cls
    if cls is not None:
        return apply_decorator(cls)
    else:
        return apply_decorator

wrap_fs_methods.method_names = ["open","exists","isdir","isfile","listdir",
    "makedir","remove","setcontents","removedir","rename","getinfo","copy",
    "move","copydir","movedir","close","getxattr","setxattr","delxattr",
    "listxattrs","validatepath","getsyspath","createfile", "hasmeta", "getmeta","listdirinfo",
    "ilistdir","ilistdirinfo"]


