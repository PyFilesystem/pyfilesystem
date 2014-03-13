"""
fs.rpcfs
========

This module provides the class 'RPCFS' to access a remote FS object over
XML-RPC.  You probably want to use this in conjunction with the 'RPCFSServer'
class from the :mod:`fs.expose.xmlrpc` module.

"""

import xmlrpclib
import socket
import base64

from fs.base import *
from fs.errors import *
from fs.path import *
from fs import iotools

from fs.filelike import StringIO

import six
from six import PY3, b


def re_raise_faults(func):
    """Decorator to re-raise XML-RPC faults as proper exceptions."""
    def wrapper(*args, **kwds):
        try:
            return func(*args, **kwds)
        except (xmlrpclib.Fault), f:
            #raise
            # Make sure it's in a form we can handle

            print f.faultString
            bits = f.faultString.split(" ")
            if bits[0] not in ["<type", "<class"]:
                raise f
            # Find the class/type object
            bits = " ".join(bits[1:]).split(">:")
            cls = bits[0]
            msg = ">:".join(bits[1:])
            cls = cls.strip('\'')
            print "-" + cls
            cls = _object_by_name(cls)
            # Re-raise using the remainder of the fault code as message
            if cls:
                if issubclass(cls, FSError):
                    raise cls('', msg=msg)
                else:
                    raise cls(msg)
            raise f
        except socket.error, e:
            raise RemoteConnectionError(str(e), details=e)
    return wrapper


def _object_by_name(name, root=None):
    """Look up an object by dotted-name notation."""
    bits = name.split(".")
    if root is None:
        try:
            obj = globals()[bits[0]]
        except KeyError:
            try:
                obj = __builtins__[bits[0]]
            except KeyError:
                obj = __import__(bits[0], globals())
    else:
        obj = getattr(root, bits[0])
    if len(bits) > 1:
        return _object_by_name(".".join(bits[1:]), obj)
    else:
        return obj


class ReRaiseFaults:
    """XML-RPC proxy wrapper that re-raises Faults as proper Exceptions."""

    def __init__(self, obj):
        self._obj = obj

    def __getattr__(self, attr):
        val = getattr(self._obj, attr)
        if callable(val):
            val = re_raise_faults(val)
            self.__dict__[attr] = val
        return val


class RPCFS(FS):
    """Access a filesystem exposed via XML-RPC.

    This class provides the client-side logic for accessing a remote FS
    object, and is dual to the RPCFSServer class defined in fs.expose.xmlrpc.

    Example::

        fs = RPCFS("http://my.server.com/filesystem/location/")

    """

    _meta = {'thread_safe' : True,
             'virtual': False,
             'network' : True,
              }

    def __init__(self, uri, transport=None):
        """Constructor for RPCFS objects.

        The only required argument is the URI of the server to connect
        to.  This will be passed to the underlying XML-RPC server proxy
        object, along with the 'transport' argument if it is provided.

        :param uri: address of the server

        """
        super(RPCFS, self).__init__(thread_synchronize=True)
        self.uri = uri
        self._transport = transport
        self.proxy = self._make_proxy()
        self.isdir('/')

    @synchronize
    def _make_proxy(self):
        kwds = dict(allow_none=True, use_datetime=True)

        if self._transport is not None:
            proxy = xmlrpclib.ServerProxy(self.uri, self._transport, **kwds)
        else:
            proxy = xmlrpclib.ServerProxy(self.uri, **kwds)

        return ReRaiseFaults(proxy)

    def __str__(self):
        return '<RPCFS: %s>' % (self.uri,)

    def __repr__(self):
        return '<RPCFS: %s>' % (self.uri,)

    @synchronize
    def __getstate__(self):
        state = super(RPCFS, self).__getstate__()
        try:
            del state['proxy']
        except KeyError:
            pass
        return state

    def __setstate__(self, state):
        super(RPCFS, self).__setstate__(state)
        self.proxy = self._make_proxy()

    def encode_path(self, path):
        """Encode a filesystem path for sending over the wire.

        Unfortunately XMLRPC only supports ASCII strings, so this method
        must return something that can be represented in ASCII.  The default
        is base64-encoded UTF8.
        """
        return six.text_type(base64.b64encode(path.encode("utf8")), 'ascii')

    def decode_path(self, path):
        """Decode paths arriving over the wire."""
        return six.text_type(base64.b64decode(path.encode('ascii')), 'utf8')

    @synchronize
    def getmeta(self, meta_name, default=NoDefaultMeta):
        if default is NoDefaultMeta:
            meta = self.proxy.getmeta(meta_name)
        else:
            meta = self.proxy.getmeta_default(meta_name, default)
        if isinstance(meta, basestring):
            #  To allow transport of meta with invalid xml chars (like null)
            meta = self.encode_path(meta)
        return meta

    @synchronize
    def hasmeta(self, meta_name):
        return self.proxy.hasmeta(meta_name)

    @synchronize
    @iotools.filelike_to_stream
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        # TODO: chunked transport of large files
        epath = self.encode_path(path)
        if "w" in mode:
            self.proxy.set_contents(epath, xmlrpclib.Binary(b("")))
        if "r" in mode or "a" in mode or "+" in mode:
            try:
                data = self.proxy.get_contents(epath, "rb").data
            except IOError:
                if "w" not in mode and "a" not in mode:
                    raise ResourceNotFoundError(path)
                if not self.isdir(dirname(path)):
                    raise ParentDirectoryMissingError(path)
                self.proxy.set_contents(path, xmlrpclib.Binary(b("")))
        else:
            data = b("")
        f = StringIO(data)
        if "a" not in mode:
            f.seek(0, 0)
        else:
            f.seek(0, 2)
        oldflush = f.flush
        oldclose = f.close
        oldtruncate = f.truncate

        def newflush():
            self._lock.acquire()
            try:
                oldflush()
                self.proxy.set_contents(epath, xmlrpclib.Binary(f.getvalue()))
            finally:
                self._lock.release()

        def newclose():
            self._lock.acquire()
            try:
                f.flush()
                oldclose()
            finally:
                self._lock.release()

        def newtruncate(size=None):
            self._lock.acquire()
            try:
                oldtruncate(size)
                f.flush()
            finally:
                self._lock.release()

        f.flush = newflush
        f.close = newclose
        f.truncate = newtruncate
        return f

    @synchronize
    def exists(self, path):
        path = self.encode_path(path)
        return self.proxy.exists(path)

    @synchronize
    def isdir(self, path):
        path = self.encode_path(path)
        return self.proxy.isdir(path)

    @synchronize
    def isfile(self, path):
        path = self.encode_path(path)
        return self.proxy.isfile(path)

    @synchronize
    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        enc_path = self.encode_path(path)
        if not callable(wildcard):
            entries = self.proxy.listdir(enc_path,
                                         wildcard,
                                         full,
                                         absolute,
                                         dirs_only,
                                         files_only)
            entries = [self.decode_path(e) for e in entries]
        else:
            entries = self.proxy.listdir(enc_path,
                                         None,
                                         False,
                                         False,
                                         dirs_only,
                                         files_only)
            entries = [self.decode_path(e) for e in entries]
            entries = [e for e in entries if wildcard(e)]
            if full:
                entries = [relpath(pathjoin(path, e)) for e in entries]
            elif absolute:
                entries = [abspath(pathjoin(path, e)) for e in entries]
        return entries

    @synchronize
    def makedir(self, path, recursive=False, allow_recreate=False):
        path = self.encode_path(path)
        return self.proxy.makedir(path, recursive, allow_recreate)

    @synchronize
    def remove(self, path):
        path = self.encode_path(path)
        return self.proxy.remove(path)

    @synchronize
    def removedir(self, path, recursive=False, force=False):
        path = self.encode_path(path)
        return self.proxy.removedir(path, recursive, force)

    @synchronize
    def rename(self, src, dst):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.rename(src, dst)

    @synchronize
    def settimes(self, path, accessed_time, modified_time):
        path = self.encode_path(path)
        return self.proxy.settimes(path, accessed_time, modified_time)

    @synchronize
    def getinfo(self, path):
        path = self.encode_path(path)
        info = self.proxy.getinfo(path)
        return info

    @synchronize
    def desc(self, path):
        path = self.encode_path(path)
        return self.proxy.desc(path)

    @synchronize
    def getxattr(self, path, attr, default=None):
        path = self.encode_path(path)
        attr = self.encode_path(attr)
        return self.fs.getxattr(path, attr, default)

    @synchronize
    def setxattr(self, path, attr, value):
        path = self.encode_path(path)
        attr = self.encode_path(attr)
        return self.fs.setxattr(path, attr, value)

    @synchronize
    def delxattr(self, path, attr):
        path = self.encode_path(path)
        attr = self.encode_path(attr)
        return self.fs.delxattr(path, attr)

    @synchronize
    def listxattrs(self, path):
        path = self.encode_path(path)
        return [self.decode_path(a) for a in self.fs.listxattrs(path)]

    @synchronize
    def copy(self, src, dst, overwrite=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.copy(src, dst, overwrite, chunk_size)

    @synchronize
    def move(self, src, dst, overwrite=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.move(src, dst, overwrite, chunk_size)

    @synchronize
    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.movedir(src, dst, overwrite, ignore_errors, chunk_size)

    @synchronize
    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.copydir(src, dst, overwrite, ignore_errors, chunk_size)
