"""

  fs.rpcfs:  Client and Server to expose an FS via XML-RPC

This module provides the following pair of classes that can be used to expose
a remote filesystem using XML-RPC:

   RPCFSServer:   a subclass of SimpleXMLRPCServer that exposes the methods
                  of an FS instance via XML-RPC

   RPCFS:   a subclass of FS that delegates all filesystem operations to
            a remote server using XML-RPC.

If you need to use a more powerful server than SimpleXMLRPCServer, you can
use the RPCFSInterface class to provide an XML-RPC-compatible wrapper around
an FS object, which can then be exposed using whatever server you choose
(e.g. Twisted's XML-RPC server).

"""

import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer

from fs.base import *

from StringIO import StringIO


class ObjProxy:
    """Simple object proxy allowing us to replace read-only attributes.

    This is used to put a modified 'close' method on files returned by
    open(), such that they will be uploaded to the server when closed.
    """

    def __init__(self,obj):
        self._obj = obj

    def __getattr__(self,attr):
        return getattr(self._obj,attr)


def re_raise_faults(func):
    """Decorator to re-raise XML-RPC faults as proper exceptions."""
    def wrapper(*args,**kwds):
        try:
            return func(*args,**kwds)
        except xmlrpclib.Fault, f:
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
    object, and is dual to the RPCFSServer class also defined in this module.

    Example:

        fs = RPCFS("http://my.server.com/filesystem/location/")

    """

    def __init__(self, uri, transport=None):
        """Constructor for RPCFS objects.

        The only required argument is the uri of the server to connect
        to.  This will be passed to the underlying XML-RPC server proxy
        object along with the 'transport' argument if it is provided.
        """
        self.uri = uri
        if transport is not None:
            proxy = xmlrpclib.ServerProxy(uri,transport,allow_none=True)
        else:
            proxy = xmlrpclib.ServerProxy(uri,allow_none=True)
        self.proxy = ReRaiseFaults(proxy)

    def __str__(self):
        return '<RPCFS: %s>' % (self.uri,)

    __repr__ = __str__

    def open(self,path,mode):
        # TODO: chunked transport of large files
        if "w" in mode:
            self.proxy.set_contents(path,xmlrpclib.Binary(""))
        if "r" in mode or "a" in mode or "+" in mode:
            try:
                data = self.proxy.get_contents(path).data
            except IOError:
                if "w" not in mode and "a" not in mode:
                    raise ResourceNotFoundError("NO_FILE",path)
                if not self.isdir(dirname(path)):
                    raise OperationFailedError("OPEN_FAILED", path,msg="Parent directory does not exist")
                self.proxy.set_contents(path,xmlrpclib.Binary(""))
        else:
            data = ""
        f = ObjProxy(StringIO(data))
        if "a" not in mode:
            f.seek(0,0)
        else:
            f.seek(0,2)
        oldflush = f.flush
        oldclose = f.close
        def newflush():
            oldflush()
            self.proxy.set_contents(path,xmlrpclib.Binary(f.getvalue()))
        def newclose():
            f.flush()
            oldclose()
        f.flush = newflush
        f.close = newclose
        return f

    def exists(self,path):
        return self.proxy.exists(path)

    def isdir(self,path):
        return self.proxy.isdir(path)

    def isfile(self,path):
        return self.proxy.isfile(path)

    def listdir(self,path="./",wildcard=None,full=False,absolute=False,hidden=True,dirs_only=False,files_only=False):
        return self.proxy.listdir(path,wildcard,full,absolute,hidden,dirs_only,files_only)

    def makedir(self,path,mode=0777,recursive=False,allow_recreate=False):
        return self.proxy.makedir(path,mode,recursive,allow_recreate)

    def remove(self,path):
        return self.proxy.remove(path)

    def removedir(self,path,recursive=False):
        return self.proxy.removedir(path,recursive)
        
    def rename(self,src,dst):
        return self.proxy.rename(src,dst)

    def getinfo(self,path):
        return self.proxy.getinfo(path)

    def desc(self,path):
        return self.proxy.desc(path)

    def getattr(self,path,attr):
        return self.proxy.getattr(path,attr)

    def setattr(self,path,attr,value):
        return self.proxy.setattr(path,attr,value)

    def copy(self,src,dst,overwrite=False,chunk_size=16384):
        return self.proxy.copy(src,dst,overwrite,chunk_size)

    def move(self,src,dst,chunk_size=16384):
        return self.proxy.move(src,dst,chunk_size)

    def movedir(self,src,dst,ignore_errors=False,chunk_size=16384):
        return self.proxy.movedir(src,dst,ignore_errors,chunk_size)

    def copydir(self,src,dst,ignore_errors=False,chunk_size=16384):
        return self.proxy.copydir(src,dst,ignore_errors,chunk_size)


class RPCFSInterface(object):
    """Wrapper to expose an FS via a XML-RPC compatible interface.

    The only real trick is using xmlrpclib.Binary objects to trasnport
    the contents of files.
    """

    def __init__(self,fs):
        self.fs = fs

    def get_contents(self,path):
        data = self.fs.getcontents(path)
        return xmlrpclib.Binary(data)

    def set_contents(self,path,data):
        self.fs.createfile(path,data.data)

    def exists(self,path):
        return self.fs.exists(path)

    def isdir(self,path):
        return self.fs.isdir(path)

    def isfile(self,path):
        return self.fs.isfile(path)

    def listdir(self,path="./",wildcard=None,full=False,absolute=False,hidden=True,dirs_only=False,files_only=False):
        return list(self.fs.listdir(path,wildcard,full,absolute,hidden,dirs_only,files_only))

    def makedir(self,path,mode=0777,recursive=False,allow_recreate=False):
        return self.fs.makedir(path,mode,recursive,allow_recreate)

    def remove(self,path):
        return self.fs.remove(path)

    def removedir(self,path,recursive=False):
        return self.fs.removedir(path,recursive)
        
    def rename(self,src,dst):
        return self.fs.rename(src,dst)

    def getinfo(self,path):
        return self.fs.getinfo(path)

    def desc(self,path):
        return self.fs.desc(path)

    def getattr(self,path,attr):
        return self.fs.getattr(path,attr)

    def setattr(self,path,attr,value):
        return self.fs.setattr(path,attr,value)

    def copy(self,src,dst,overwrite=False,chunk_size=16384):
        return self.fs.copy(src,dst,overwrite,chunk_size)

    def move(self,src,dst,chunk_size=16384):
        return self.fs.move(src,dst,chunk_size)

    def movedir(self,src,dst,ignore_errors=False,chunk_size=16384):
        return self.fs.movedir(src,dst,ignore_errors,chunk_size)

    def copydir(self,src,dst,ignore_errors=False,chunk_size=16384):
        return self.fs.copydir(src,dst,ignore_errors,chunk_size)


class RPCFSServer(SimpleXMLRPCServer):
    """Server to expose an FS object via XML-RPC.

    This class takes as its first argument an FS instance, and as its second
    argument a (hostname,port) tuple on which to listen for XML-RPC requests.
    Example:

        fs = OSFS('/var/srv/myfiles')
        s = RPCFSServer(fs,("",8080))
        s.serve_forever()

    To cleanly shut down the server after calling serve_forever, set the
    attribute "serve_more_requests" to False.
    """

    def __init__(self,fs,addr,requestHandler=None,logRequests=None):
        kwds = dict(allow_none=True)
        if requestHandler is not None:
            kwds['requestHandler'] = requestHandler
        if logRequests is not None:
            kwds['logRequests'] = logRequests
        self.serve_more_requests = True
        SimpleXMLRPCServer.__init__(self,addr,**kwds)
        self.register_instance(RPCFSInterface(fs))

    def serve_forever(self):
        """Override serve_forever to allow graceful shutdown."""
        while self.serve_more_requests:
            self.handle_request()

