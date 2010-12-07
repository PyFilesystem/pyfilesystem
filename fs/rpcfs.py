"""
fs.rpcfs
========

This module provides the class 'RPCFS' to access a remote FS object over
XML-RPC.  You probably want to use this in conjunction with the 'RPCFSServer'
class from the :mod:`fs.expose.xmlrpc` module.

"""

import xmlrpclib
import socket

from fs.base import *
from fs.errors import *
from fs.path import *

from fs.filelike import StringIO


def re_raise_faults(func):
    """Decorator to re-raise XML-RPC faults as proper exceptions."""
    def wrapper(*args,**kwds):        
        try:
            return func(*args,**kwds)
        except xmlrpclib.Fault, f:
            import traceback
            traceback.print_exc()           
            # Make sure it's in a form we can handle
            bits = f.faultString.split(" ")
            if bits[0] not in ["<type","<class"]:
                raise f
            # Find the class/type object
            bits = " ".join(bits[1:]).split(">:")
            cls = bits[0]
            msg = ">:".join(bits[1:])
            while cls[0] in ["'",'"']:
                cls = cls[1:]
            while cls[-1] in ["'",'"']:
                cls = cls[:-1]
            cls = _object_by_name(cls)
            # Re-raise using the remainder of the fault code as message
            if cls:
                raise cls(msg)
            raise f
        except socket.error, e:
            raise RemoteConnectionError(str(e), details=e)
    return wrapper


def _object_by_name(name,root=None):
    """Look up an object by dotted-name notation."""
    bits = name.split(".")
    if root is None:
        try:
            obj = globals()[bits[0]]
        except KeyError:
            try:
                obj = __builtins__[bits[0]]
            except KeyError:
                obj = __import__(bits[0],globals())
    else:
        obj = getattr(root,bits[0])
    if len(bits) > 1:
        return _object_by_name(".".join(bits[1:]),obj)
    else:
        return obj
    

class ReRaiseFaults:
    """XML-RPC proxy wrapper that re-raises Faults as proper Exceptions."""

    def __init__(self,obj):
        self._obj = obj

    def __getattr__(self,attr):
        val = getattr(self._obj,attr)
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

    _meta = { 'virtual': False,                                          
              'network' : True,              
              }

    def __init__(self, uri, transport=None):
        """Constructor for RPCFS objects.

        The only required argument is the uri of the server to connect
        to.  This will be passed to the underlying XML-RPC server proxy
        object, along with the 'transport' argument if it is provided.
        
        :param uri: address of the server        
        
        """
        self.uri = uri
        self._transport = transport
        self.proxy = self._make_proxy()
        FS.__init__(self,thread_synchronize=False)

    def _make_proxy(self):
        kwds = dict(allow_none=True)
        
        if self._transport is not None:
            proxy = xmlrpclib.ServerProxy(self.uri,self._transport,**kwds)
        else:
            proxy = xmlrpclib.ServerProxy(self.uri,**kwds)            
    
        return ReRaiseFaults(proxy)

    def __str__(self):
        return '<RPCFS: %s>' % (self.uri,)

    def __getstate__(self):
        state = super(RPCFS,self).__getstate__()
        try:
            del state['proxy']
        except KeyError:
            pass
        return state

    def __setstate__(self, state):
        for (k,v) in state.iteritems():
            self.__dict__[k] = v
        self.proxy = self._make_proxy()

    def encode_path(self, path):
        """Encode a filesystem path for sending over the wire.

        Unfortunately XMLRPC only supports ASCII strings, so this method
        must return something that can be represented in ASCII.  The default
        is base64-encoded UTF8.
        """
        return path.encode("utf8").encode("base64")

    def decode_path(self, path):
        """Decode paths arriving over the wire."""
        return path.decode("base64").decode("utf8")
    
    def getmeta(self, meta_name, default=NoDefaultMeta):
        if default is NoDefaultMeta:                
            return self.proxy.getmeta(meta_name)
        else:
            return self.proxy.getmeta_default(meta_name, default)                     
    
    def hasmeta(self, meta_name):        
        return self.proxy.hasmeta(meta_name)

    def open(self, path, mode="r"):
        # TODO: chunked transport of large files
        path = self.encode_path(path)
        if "w" in mode:
            self.proxy.set_contents(path,xmlrpclib.Binary(""))
        if "r" in mode or "a" in mode or "+" in mode:
            try:
                data = self.proxy.get_contents(path).data
            except IOError:
                if "w" not in mode and "a" not in mode:
                    raise ResourceNotFoundError(path)
                if not self.isdir(dirname(path)):
                    raise ParentDirectoryMissingError(path)
                self.proxy.set_contents(path,xmlrpclib.Binary(""))
        else:
            data = ""
        f = StringIO(data)
        if "a" not in mode:
            f.seek(0,0)
        else:
            f.seek(0,2)
        oldflush = f.flush
        oldclose = f.close
        oldtruncate = f.truncate
        def newflush():
            oldflush()
            self.proxy.set_contents(path,xmlrpclib.Binary(f.getvalue()))
        def newclose():
            f.flush()
            oldclose()
        def newtruncate(size=None):
            oldtruncate(size)
            f.flush()
        f.flush = newflush
        f.close = newclose
        f.truncate = newtruncate
        return f

    def exists(self, path):
        path = self.encode_path(path)
        return self.proxy.exists(path)

    def isdir(self, path):
        path = self.encode_path(path)
        return self.proxy.isdir(path)

    def isfile(self, path):
        path = self.encode_path(path)
        return self.proxy.isfile(path)

    def listdir(self, path="./", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        path = self.encode_path(path)
        entries =  self.proxy.listdir(path,wildcard,full,absolute,dirs_only,files_only)
        return [self.decode_path(e) for e in entries]

    def makedir(self, path, recursive=False, allow_recreate=False):
        path = self.encode_path(path)
        return self.proxy.makedir(path,recursive,allow_recreate)

    def remove(self, path):
        path = self.encode_path(path)
        return self.proxy.remove(path)

    def removedir(self, path, recursive=False, force=False):
        path = self.encode_path(path)
        return self.proxy.removedir(path,recursive,force)
        
    def rename(self, src, dst):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.rename(src,dst)

    def settimes(self, path, accessed_time, modified_time):
        path = self.encode_path(path)
        return self.proxy.settimes(path, accessed_time, modified_time)

    def getinfo(self, path):
        path = self.encode_path(path)
        return self.proxy.getinfo(path)

    def desc(self, path):
        path = self.encode_path(path)
        return self.proxy.desc(path)

    def getxattr(self, path, attr, default=None):
        path = self.encode_path(path)
        attr = self.encode_path(attr)
        return self.fs.getxattr(path,attr,default)

    def setxattr(self, path, attr, value):
        path = self.encode_path(path)
        attr = self.encode_path(attr)
        return self.fs.setxattr(path,attr,value)

    def delxattr(self, path, attr):
        path = self.encode_path(path)
        attr = self.encode_path(attr)
        return self.fs.delxattr(path,attr)

    def listxattrs(self, path):
        path = self.encode_path(path)
        return [self.decode_path(a) for a in self.fs.listxattrs(path)]

    def copy(self, src, dst, overwrite=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.copy(src,dst,overwrite,chunk_size)

    def move(self, src, dst, overwrite=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.move(src,dst,overwrite,chunk_size)

    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.movedir(src, dst, overwrite, ignore_errors, chunk_size)

    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        src = self.encode_path(src)
        dst = self.encode_path(dst)
        return self.proxy.copydir(src,dst,overwrite,ignore_errors,chunk_size)


