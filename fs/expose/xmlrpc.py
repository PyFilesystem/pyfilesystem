"""

  fs.expose.xmlrpc:  server to expose an FS via XML-RPC

This module provides the necessary infrastructure to expose an FS object
over XML-RPC.  The main class is 'RPCFSServer', a SimpleXMLRPCServer subclass
designed to expose an underlying FS.

If you need to use a more powerful server than SimpleXMLRPCServer, you can
use the RPCFSInterface class to provide an XML-RPC-compatible wrapper around
an FS object, which can then be exposed using whatever server you choose
(e.g. Twisted's XML-RPC server).

"""

import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer


class RPCFSInterface(object):
    """Wrapper to expose an FS via a XML-RPC compatible interface.

    The only real trick is using xmlrpclib.Binary objects to transport
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

    def listdir(self,path="./",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        return list(self.fs.listdir(path,wildcard,full,absolute,dirs_only,files_only))

    def makedir(self,path,recursive=False,allow_recreate=False):
        return self.fs.makedir(path,recursive,allow_recreate)

    def remove(self,path):
        return self.fs.remove(path)

    def removedir(self,path,recursive=False,force=False):
        return self.fs.removedir(path,recursive,force)
        
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

    def move(self,src,dst,overwrite=False,chunk_size=16384):
        return self.fs.move(src,dst,overwrite,chunk_size)

    def movedir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=16384):
        return self.fs.movedir(src,dst,overwrite,ignore_errors,chunk_size)

    def copydir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=16384):
        return self.fs.copydir(src,dst,overwrite,ignore_errors,chunk_size)


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

