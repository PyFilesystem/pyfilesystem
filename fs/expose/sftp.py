"""
fs.expose.sftp
==============

Expose an FS object over SFTP (via paramiko).

This module provides the necessary interfaces to expose an FS object over
SFTP, plugging into the infrastructure provided by the 'paramiko' module.

For simple usage, the class 'BaseSFTPServer' provides an all-in-one server
class based on the standard SocketServer module.  Use it like so::

    server = BaseSFTPServer((hostname,port),fs)
    server.serve_forever()

Note that the base class allows UNAUTHENTICATED ACCESS by default.  For more
serious work you will probably want to subclass it and override methods such
as check_auth_password() and get_allowed_auths().

To integrate this module into an existing server framework based on paramiko,
the 'SFTPServerInterface' class provides a concrete implementation of the
paramiko.SFTPServerInterface protocol.  If you don't understand what this
is, you probably don't want to use it.

"""

from __future__ import with_statement

import os
import stat as statinfo
import time
import SocketServer
import threading

import paramiko

from fs.base import flags_to_mode
from fs.path import *
from fs.errors import *
from fs.local_functools import wraps
from fs.filelike import StringIO
from fs.utils import isdir


# Default host key used by BaseSFTPServer
#
DEFAULT_HOST_KEY = paramiko.RSAKey.from_private_key(StringIO(
    "-----BEGIN RSA PRIVATE KEY-----\n" \
    "MIICXgIBAAKCAIEAl7sAF0x2O/HwLhG68b1uG8KHSOTqe3Cdlj5i/1RhO7E2BJ4B\n" \
    "3jhKYDYtupRnMFbpu7fb21A24w3Y3W5gXzywBxR6dP2HgiSDVecoDg2uSYPjnlDk\n" \
    "HrRuviSBG3XpJ/awn1DObxRIvJP4/sCqcMY8Ro/3qfmid5WmMpdCZ3EBeC0CAwEA\n" \
    "AQKCAIBSGefUs5UOnr190C49/GiGMN6PPP78SFWdJKjgzEHI0P0PxofwPLlSEj7w\n" \
    "RLkJWR4kazpWE7N/bNC6EK2pGueMN9Ag2GxdIRC5r1y8pdYbAkuFFwq9Tqa6j5B0\n" \
    "GkkwEhrcFNBGx8UfzHESXe/uE16F+e8l6xBMcXLMJVo9Xjui6QJBAL9MsJEx93iO\n" \
    "zwjoRpSNzWyZFhiHbcGJ0NahWzc3wASRU6L9M3JZ1VkabRuWwKNuEzEHNK8cLbRl\n" \
    "TyH0mceWXcsCQQDLDEuWcOeoDteEpNhVJFkXJJfwZ4Rlxu42MDsQQ/paJCjt2ONU\n" \
    "WBn/P6iYDTvxrt/8+CtLfYc+QQkrTnKn3cLnAkEAk3ixXR0h46Rj4j/9uSOfyyow\n" \
    "qHQunlZ50hvNz8GAm4TU7v82m96449nFZtFObC69SLx/VsboTPsUh96idgRrBQJA\n" \
    "QBfGeFt1VGAy+YTLYLzTfnGnoFQcv7+2i9ZXnn/Gs9N8M+/lekdBFYgzoKN0y4pG\n" \
    "2+Q+Tlr2aNlAmrHtkT13+wJAJVgZATPI5X3UO0Wdf24f/w9+OY+QxKGl86tTQXzE\n" \
    "4bwvYtUGufMIHiNeWP66i6fYCucXCMYtx6Xgu2hpdZZpFw==\n" \
    "-----END RSA PRIVATE KEY-----\n"
))


def report_sftp_errors(func):
    """Decorator to catch and report FS errors as SFTP error codes.

    Any FSError exceptions are caught and translated into an appropriate
    return code, while other exceptions are passed through untouched.
    """
    @wraps(func)
    def wrapper(*args,**kwds):
        try:
            return func(*args, **kwds)
        except ResourceNotFoundError, e:
            return paramiko.SFTP_NO_SUCH_FILE
        except UnsupportedError, e:
            return paramiko.SFTP_OP_UNSUPPORTED
        except FSError, e:
            return paramiko.SFTP_FAILURE
    return wrapper


class SFTPServerInterface(paramiko.SFTPServerInterface):
    """SFTPServerInterface implementation that exposes an FS object.

    This SFTPServerInterface subclass expects a single additional argument,
    the fs object to be exposed.  Use it to set up a transport subsystem
    handler like so::

      t.set_subsystem_handler("sftp",SFTPServer,SFTPServerInterface,fs)

    If this all looks too complicated, you might consider the BaseSFTPServer
    class also provided by this module - it automatically creates the enclosing
    paramiko server infrastructure.
    """

    def __init__(self, server, fs, encoding=None, *args, **kwds):
        self.fs = fs
        if encoding is None:
            encoding = "utf8"
        self.encoding = encoding
        super(SFTPServerInterface,self).__init__(server, *args, **kwds)

    def close(self):
        # Close the pyfs file system and dereference it.
        self.fs.close()
        self.fs = None

    @report_sftp_errors
    def open(self, path, flags, attr):
        return SFTPHandle(self, path, flags)

    @report_sftp_errors
    def list_folder(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        stats = []
        for entry in self.fs.listdir(path, absolute=True):
            stat = self.stat(entry)
            if not isinstance(stat, int):
                stats.append(stat)
        return stats

    @report_sftp_errors
    def stat(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)

        info = self.fs.getinfo(path)

        stat = paramiko.SFTPAttributes()
        stat.filename = basename(path).encode(self.encoding)
        stat.st_size = info.get("size")

        if 'st_atime' in info:
            stat.st_atime = info.get('st_atime')
        elif 'accessed_time' in info:
            stat.st_atime = time.mktime(info.get("accessed_time").timetuple())

        if 'st_mtime' in info:
            stat.st_mtime = info.get('st_mtime')
        else:
            if 'modified_time' in info:
                stat.st_mtime = time.mktime(info.get("modified_time").timetuple())

        if isdir(self.fs, path, info):
            stat.st_mode = 0777 | statinfo.S_IFDIR
        else:
            stat.st_mode = 0777 | statinfo.S_IFREG
        return stat

    def lstat(self, path):
        return self.stat(path)

    @report_sftp_errors
    def remove(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        self.fs.remove(path)
        return paramiko.SFTP_OK

    @report_sftp_errors
    def rename(self, oldpath, newpath):
        if not isinstance(oldpath, unicode):
            oldpath = oldpath.decode(self.encoding)
        if not isinstance(newpath, unicode):
            newpath = newpath.decode(self.encoding)
        if self.fs.isfile(oldpath):
            self.fs.move(oldpath, newpath)
        else:
            self.fs.movedir(oldpath, newpath)
        return paramiko.SFTP_OK

    @report_sftp_errors
    def mkdir(self, path, attr):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        self.fs.makedir(path)
        return paramiko.SFTP_OK

    @report_sftp_errors
    def rmdir(self, path):
        if not isinstance(path, unicode):
            path = path.decode(self.encoding)
        self.fs.removedir(path)
        return paramiko.SFTP_OK

    def canonicalize(self, path):
        try:
            return abspath(normpath(path)).encode(self.encoding)
        except BackReferenceError:
            # If the client tries to use backrefs to escape root, gently
            # nudge them back to /.
            return '/'

    @report_sftp_errors
    def chattr(self, path, attr):
        #  f.truncate() is implemented by setting the size attr.
        #  Any other attr requests fail out.
        if attr._flags:
            if attr._flags != attr.FLAG_SIZE:
                raise UnsupportedError
            with self.fs.open(path,"r+") as f:
                f.truncate(attr.st_size)
        return paramiko.SFTP_OK

    def readlink(self, path):
        return paramiko.SFTP_OP_UNSUPPORTED

    def symlink(self, path):
        return paramiko.SFTP_OP_UNSUPPORTED


class SFTPHandle(paramiko.SFTPHandle):
    """SFTP file handler pointing to a file in an FS object.

    This is a simple file wrapper for SFTPServerInterface, passing read
    and write requests directly through the to underlying file from the FS.
    """

    def __init__(self, owner, path, flags):
        super(SFTPHandle, self).__init__(flags)
        mode = flags_to_mode(flags)
        self.owner = owner
        if not isinstance(path, unicode):
            path = path.decode(self.owner.encoding)
        self.path = path
        self._file = owner.fs.open(path, mode)

    @report_sftp_errors
    def close(self):
        self._file.close()
        return paramiko.SFTP_OK

    @report_sftp_errors
    def read(self, offset, length):
        self._file.seek(offset)
        return self._file.read(length)

    @report_sftp_errors
    def write(self, offset, data):
        self._file.seek(offset)
        self._file.write(data)
        return paramiko.SFTP_OK

    def stat(self):
        return self.owner.stat(self.path)

    def chattr(self,attr):
        return self.owner.chattr(self.path, attr)


class SFTPServer(paramiko.SFTPServer):
    """
    An SFTPServer class that closes the filesystem when done.
    """

    def finish_subsystem(self):
        # Close the SFTPServerInterface, it will close the pyfs file system.
        self.server.close()
        super(SFTPServer, self).finish_subsystem()


class SFTPRequestHandler(SocketServer.BaseRequestHandler):
    """SocketServer RequestHandler subclass for BaseSFTPServer.

    This RequestHandler subclass creates a paramiko Transport, sets up the
    sftp subsystem, and hands off to the transport's own request handling
    thread.
    """
    timeout = 60
    auth_timeout = 60

    def setup(self):
        """
        Creates the SSH transport. Sets security options.
        """
        self.transport = paramiko.Transport(self.request)
        self.transport.load_server_moduli()
        so = self.transport.get_security_options()
        so.digests = ('hmac-sha1', )
        so.compression = ('zlib@openssh.com', 'none')
        self.transport.add_server_key(self.server.host_key)
        self.transport.set_subsystem_handler("sftp", SFTPServer, SFTPServerInterface, self.server.fs, encoding=self.server.encoding)

    def handle(self):
        """
        Start the paramiko server, this will start a thread to handle the connection.
        """
        self.transport.start_server(server=BaseServerInterface())
        # TODO: I like the code below _in theory_ but it does not work as I expected.
        # Figure out how to actually time out a new client if they fail to auth in a
        # certain amount of time.
        #chan = self.transport.accept(self.auth_timeout)
        #if chan is None:
        #    self.transport.close()

    def handle_timeout(self):
        try:
            self.transport.close()
        finally:
            super(SFTPRequestHandler, self).handle_timeout()



class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


class BaseSFTPServer(ThreadedTCPServer):
    """SocketServer.TCPServer subclass exposing an FS via SFTP.

    Operation is in the standard SocketServer style.  The target FS object
    can be passed into the constructor, or set as an attribute on the server::

        server = BaseSFTPServer((hostname,port),fs)
        server.serve_forever()

    It is also possible to specify the host key used by the sever by setting
    the 'host_key' attribute.  If this is not specified, it will default to
    the key found in the DEFAULT_HOST_KEY variable.
    """
    # If the server stops/starts quickly, don't fail because of
    # "port in use" error.
    allow_reuse_address = True

    def __init__(self, address, fs=None, encoding=None, host_key=None, RequestHandlerClass=None):
        self.fs = fs
        self.encoding = encoding
        if host_key is None:
            host_key = DEFAULT_HOST_KEY
        self.host_key = host_key
        if RequestHandlerClass is None:
            RequestHandlerClass = SFTPRequestHandler
        SocketServer.TCPServer.__init__(self, address, RequestHandlerClass)

    def shutdown_request(self, request):
        # Prevent TCPServer from closing the connection prematurely
        return

    def close_request(self, request):
        # Prevent TCPServer from closing the connection prematurely
        return


class BaseServerInterface(paramiko.ServerInterface):
    """
    Paramiko ServerInterface implementation that performs user authentication.

    Note that this base class allows UNAUTHENTICATED ACCESS to the exposed
    FS.  This is intentional, since we can't guess what your authentication
    needs are.  To protect the exposed FS, override the following methods:

        * get_allowed_auths Determine the allowed auth modes
        * check_auth_none Check auth with no credentials
        * check_auth_password Check auth with a password
        * check_auth_publickey Check auth with a public key
    """

    def check_channel_request(self, kind, chanid):
        if kind == 'session':
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_none(self, username):
        """Check whether the user can proceed without authentication."""
        return paramiko.AUTH_SUCCESSFUL

    def check_auth_publickey(self, username,key):
        """Check whether the given public key is valid for authentication."""
        return paramiko.AUTH_FAILED

    def check_auth_password(self, username, password):
        """Check whether the given password is valid for authentication."""
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self,username):
        """Return string containing a comma separated list of allowed auth modes.

        The available modes are  "node", "password" and "publickey".
        """
        return "none"


#  When called from the command-line, expose a TempFS for testing purposes
if __name__ == "__main__":
    from fs.tempfs import TempFS
    server = BaseSFTPServer(("localhost",8022),TempFS())
    try:
        #import rpdb2; rpdb2.start_embedded_debugger('password')
        server.serve_forever()
    except (SystemExit,KeyboardInterrupt):
        server.server_close()

