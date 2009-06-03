"""

  fs.expose.sftp:  expose an FS object via SFTP, using paramiko.

"""

import os
import stat as statinfo
import time
import SocketServer as ss
import threading

import paramiko

from fs.errors import *
from fs.helpers import *

try:
    from functools import wraps
except ImportError:
    def wraps(f):
        return f

def debug(func):
    @wraps(func)
    def wrapper(*args,**kwds):
        print func, args[1:], kwds
        try:
            res = func(*args,**kwds)
        except Exception, e:
            print "EXC:", e
            raise
        print "RES:", res
        return res
    return wrapper


def report_sftp_errors(func):
    """Decorator to catch and report FS errors as SFTP error codes."""
    @debug
    @wraps(func)
    def wrapper(*args,**kwds):
        try:
            return func(*args,**kwds)
        except ResourceNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except UnsupportedError:
            return paramiko.SFTP_OP_UNSUPPORTED
        except FSError:
            return paramiko.SFTP_FAILURE
    return wrapper


class SFTPServerInterface(paramiko.SFTPServerInterface):
    """SFTPServerInferface implementation that exposes an FS object.

    This SFTPServerInterface subclass expects a single additional argument,
    the fs object to be exposed.  Use it to set up a transport like so:

      t.set_subsystem_handler("sftp",SFTPServer,SFTPServerInterface,fs)

    """

    def __init__(self,server,fs,*args,**kwds):
        self.fs = fs
        super(SFTPServerInterface,self).__init__(server,*args,**kwds)

    @report_sftp_errors
    def open(self,path,flags,attr):
        return SFTPHandle(self,path,flags)

    @report_sftp_errors
    def list_folder(self,path):
        stats = []
        for entry in self.fs.listdir(path,absolute=True):
            stats.append(self.stat(entry))
        return stats
 
    @report_sftp_errors
    def stat(self,path):
        info = self.fs.getinfo(path)
        stat = paramiko.SFTPAttributes()
        stat.filename = resourcename(path)
        stat.st_size = info.get("size")
        stat.st_atime = time.mktime(info.get("accessed_time").timetuple())
        stat.st_mtime = time.mktime(info.get("modified_time").timetuple())
        if self.fs.isdir(path):
            stat.st_mode = 0777 | statinfo.S_IFDIR
        else:
            stat.st_mode = 0777 | statinfo.S_IFREG
        return stat

    def lstat(self,path):
        return self.stat(path)

    @report_sftp_errors
    def remove(self,path):
        self.fs.remove(path)
        return paramiko.SFTP_OK

    @report_sftp_errors
    def rename(self,oldpath,newpath):
        if self.fs.isfile(path):
            self.fs.move(oldpath,newpath)
        else:
            self.fs.movedir(oldpath,newpath)
        return paramiko.SFTP_OK

    @report_sftp_errors
    def mkdir(self,path,attr):
        self.fs.makedir(path)
        return paramiko.SFTP_OK

    @report_sftp_errors
    def rmdir(self,path):
        self.fs.removedir(path)
        return paramiko.SFTP_OK

    def canonicalize(self,path):
        return makeabsolute(path)

    def chattr(self,path,attr):
        return paramiko.SFTP_OP_UNSUPPORTED

    def readlink(self,path):
        return paramiko.SFTP_OP_UNSUPPORTED

    def symlink(self,path):
        return paramiko.SFTP_OP_UNSUPPORTED


class SFTPHandle(paramiko.SFTPHandle):
    """SFTP file handler pointing to a file in an FS object."""

    def __init__(self,owner,path,flags):
        super(SFTPHandle,self).__init__(flags)
        mode = self._flags_to_mode(flags)
        self.owner = owner
        self.path = path
        self._file = owner.fs.open(path,mode)

    def _flags_to_mode(self,flags):
        """Convert an os.O_* bitmask into an FS mode string."""
        if flags & os.O_EXCL:
            raise UnsupportedError("open",msg="O_EXCL is not supported")
        if flags & os.O_WRONLY:
            if flags & os.O_TRUNC:
                mode = "w"
            elif flags & os.O_APPEND:
                mode = "a"
            else:
                mode = "r+"
        elif flags & os.O_RDWR:
            if flags & os.O_TRUNC:
                mode = "w+"
            elif flags & os.O_APPEND:
                mode = "a+"
            else:
                mode = "r+"
        else:
            mode = "r"
        return mode

    @report_sftp_errors
    def close(self):
        self._file.close()
        return paramiko.SFTP_OK

    @report_sftp_errors
    def read(self,offset,length):
        self._file.seek(offset)
        return self._file.read(length)

    @report_sftp_errors
    def write(self,offset,data):
        self._file.seek(offset)
        self._file.write(length)
        return paramiko.SFTP_OK

    def stat(self):
        return self.owner.stat(self.path)

    def chattr(self,attr):
        return self.owner.chattr(self.path,attr)



class SFTPRequestHandler(ss.StreamRequestHandler):
    """SockerServer RequestHandler subclass for our SFTP server."""

    def handle(self):
        t = paramiko.Transport(self.request)
        t.add_server_key(self.server.host_key)
        t.set_subsystem_handler("sftp",paramiko.SFTPServer,SFTPServerInterface,self.server.fs)
        # Careful - this actually spawns a new thread to handle the requests
        t.start_server(server=self.server)


class BaseSFTPServer(ss.TCPServer,paramiko.ServerInterface):
    """SocketServer.TCPServer subclass exposing an FS via SFTP."""

    def __init__(self,address,fs=None,host_key=None,RequestHandlerClass=None):
        self.fs = fs
        if host_key is None:
            self.host_key = paramiko.RSAKey.generate(1024)
        else:
            self.host_key = host_key
        if RequestHandlerClass is None:
            RequestHandlerClass = SFTPRequestHandler
        ss.TCPServer.__init__(self,address,RequestHandlerClass)

    def close_request(self,request):
        #  paramiko.Transport closes itself when finished,
        #  so there's  no need for us to do it.
        pass

    def check_channel_request(self,kind,chanid):
        if kind == 'session':
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_none(self,username):
        if True:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def check_auth_publickey(self,username,key):
        if True:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def check_auth_password(self,username,password):
        if True:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self,username):
        return ("none","publickey","password")



def serve(addr,fs,host_key=None):
    """Serve the given FS on the given address."""
    server = BaseSFTPServer(addr,fs)
    server.serve_forever()


if __name__ == "__main__":
    from fs.tempfs import TempFS
    serve(("localhost",8023),TempFS())


