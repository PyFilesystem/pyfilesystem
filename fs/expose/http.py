__all__ = ["serve_fs"]

import SimpleHTTPServer
import SocketServer
from fs.path import pathjoin, dirname
from fs.errors import FSError
from time import mktime
from cStringIO import StringIO
import cgi
import urllib
import posixpath
import time
import threading
import socket

def _datetime_to_epoch(d):
    return mktime(d.timetuple())

class FSHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    """A hacked together version of SimpleHTTPRequestHandler"""

    def __init__(self, fs, request, client_address, server):
        self._fs = fs
        SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self, request, client_address, server)

    def do_GET(self):
        """Serve a GET request."""
        f = None
        try:
            f = self.send_head()
            if f:
                try:
                    self.copyfile(f, self.wfile)
                except socket.error:
                    pass
        finally:
            if f is not None:
                f.close()

    def send_head(self):
        """Common code for GET and HEAD commands.

        This sends the response code and MIME headers.

        Return value is either a file object (which has to be copied
        to the outputfile by the caller unless the command was HEAD,
        and must be closed by the caller under all circumstances), or
        None, in which case the caller has nothing further to do.

        """
        path = self.translate_path(self.path)
        f = None
        if self._fs.isdir(path):
            if not self.path.endswith('/'):
                # redirect browser - doing basically what apache does
                self.send_response(301)
                self.send_header("Location", self.path + "/")
                self.end_headers()
                return None
            for index in ("index.html", "index.htm"):
                index = pathjoin(path, index)
                if self._fs.exists(index):
                    path = index
                    break
            else:
                return self.list_directory(path)
        ctype = self.guess_type(path)
        try:
            info = self._fs.getinfo(path)
            f = self._fs.open(path, 'rb')
        except FSError, e:
            self.send_error(404, str(e))
            return None
        self.send_response(200)
        self.send_header("Content-type", ctype)
        self.send_header("Content-Length", str(info['size']))
        if 'modified_time' in info:
            self.send_header("Last-Modified", self.date_time_string(_datetime_to_epoch(info['modified_time'])))
        self.end_headers()
        return f


    def list_directory(self, path):
        """Helper to produce a directory listing (absent index.html).

        Return value is either a file object, or None (indicating an
        error).  In either case, the headers are sent, making the
        interface the same as for send_head().

        """
        try:
            dir_paths = self._fs.listdir(path, dirs_only=True)
            file_paths = self._fs.listdir(path, files_only=True)
        except FSError:
            self.send_error(404, "No permission to list directory")
            return None
        paths = [p+'/' for p in sorted(dir_paths, key=lambda p:p.lower())] + sorted(file_paths, key=lambda p:p.lower())
        #list.sort(key=lambda a: a.lower())
        f = StringIO()
        displaypath = cgi.escape(urllib.unquote(self.path))
        f.write('<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">')
        f.write("<html>\n<title>Directory listing for %s</title>\n" % displaypath)
        f.write("<body>\n<h2>Directory listing for %s</h2>\n" % displaypath)
        f.write("<hr>\n<ul>\n")

        parent = dirname(path)
        if path != parent:
            f.write('<li><a href="%s">../</a></li>' % urllib.quote(parent.rstrip('/') + '/'))

        for path in paths:
            f.write('<li><a href="%s">%s</a>\n'
                    % (urllib.quote(path), cgi.escape(path)))
        f.write("</ul>\n<hr>\n</body>\n</html>\n")
        length = f.tell()
        f.seek(0)
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.send_header("Content-Length", str(length))
        self.end_headers()
        return f

    def translate_path(self, path):
        # abandon query parameters
        path = path.split('?',1)[0]
        path = path.split('#',1)[0]
        path = posixpath.normpath(urllib.unquote(path))
        return path


def serve_fs(fs, address='', port=8000):

    """Serve an FS instance over http

    :param fs: an FS object
    :param address: IP address to serve on
    :param port: port number

    """

    def Handler(request, client_address, server):
        return FSHTTPRequestHandler(fs, request, client_address, server)

    #class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    #    pass
    httpd = SocketServer.TCPServer((address, port), Handler, bind_and_activate=False)
    #httpd = ThreadedTCPServer((address, port), Handler, bind_and_activate=False)
    httpd.allow_reuse_address = True
    httpd.server_bind()
    httpd.server_activate()

    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.start()
    try:
        while True:
            time.sleep(0.1)
    except (KeyboardInterrupt, SystemExit):
        httpd.shutdown()

if __name__ == "__main__":

    from fs.osfs import OSFS
    serve_fs(OSFS('~/'))