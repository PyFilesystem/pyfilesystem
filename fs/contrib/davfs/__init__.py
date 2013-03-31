"""
fs.contrib.davfs
================


FS implementation accessing a WebDAV server.

This module provides a relatively-complete WebDAV Level 1 client that exposes
a WebDAV server as an FS object.  Locks are not currently supported.

Requires the dexml module:

    http://pypi.python.org/pypi/dexml/

"""
#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  All rights reserved; available under the terms of the MIT License.

from __future__ import with_statement

import os
import sys
import httplib
import socket
from urlparse import urlparse
import stat as statinfo
from urllib import quote as urlquote
from urllib import unquote as urlunquote
import base64
import re
import time
import datetime
import cookielib
import fnmatch
import xml.dom.pulldom
import threading
from collections import deque

import fs
from fs.base import *
from fs.path import *
from fs.errors import *
from fs.remote import RemoteFileBuffer
from fs import iotools

from fs.contrib.davfs.util import *
from fs.contrib.davfs import xmlobj
from fs.contrib.davfs.xmlobj import *

import six
from six import b

import errno
_RETRYABLE_ERRORS = [errno.EADDRINUSE]
try:
    _RETRYABLE_ERRORS.append(errno.ECONNRESET)
    _RETRYABLE_ERRORS.append(errno.ECONNABORTED)
except AttributeError:
    _RETRYABLE_ERRORS.append(104)



class DAVFS(FS):
    """Access a remote filesystem via WebDAV.

    This FS implementation provides access to a remote filesystem via the
    WebDAV protocol.  Basic Level 1 WebDAV is supported; locking is not
    currently supported, but planned for the future.

    HTTP Basic authentication is supported; provide a dict giving username
    and password in the "credentials" argument, or a callback for obtaining
    one in the "get_credentials" argument.

    To use custom HTTP connector classes (e.g. to implement proper certificate
    checking for SSL connections) you can replace the factory functions in the
    DAVFS.connection_classes dictionary, or provide the "connection_classes"
    argument.
    """

    connection_classes = {
        "http":  httplib.HTTPConnection,
        "https":  httplib.HTTPSConnection,
    }

    _DEFAULT_PORT_NUMBERS = {
        "http": 80,
        "https": 443,
    }

    _meta = { 'virtual' : False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'network' : True
             }

    def __init__(self,url,credentials=None,get_credentials=None,thread_synchronize=True,connection_classes=None,timeout=None):
        """DAVFS constructor.

        The only required argument is the root url of the remote server. If
        authentication is required, provide the 'credentials' keyword argument
        and/or the 'get_credentials' keyword argument.  The former is a dict
        of credentials info, while the latter is a callback function returning
        such a dict. Only HTTP Basic Auth is supported at this stage, so the
        only useful keys in a credentials dict are 'username' and 'password'.
        """
        if not url.endswith("/"):
            url = url + "/"
        self.url = url
        self.timeout = timeout
        self.credentials = credentials
        self.get_credentials = get_credentials
        if connection_classes is not None:
            self.connection_classes = self.connection_classes.copy()
            self.connection_classes.update(connection_classes)
        self._connections = []
        self._free_connections = {}
        self._connection_lock = threading.Lock()
        self._cookiejar = cookielib.CookieJar()
        super(DAVFS,self).__init__(thread_synchronize=thread_synchronize)
        #  Check that the server speaks WebDAV, and normalize the URL
        #  after any redirects have been followed.
        self.url = url
        pf = propfind(prop="<prop xmlns='DAV:'><resourcetype /></prop>")
        resp = self._request("/","PROPFIND",pf.render(),{"Depth":"0"})
        try:
            if resp.status == 404:
                raise ResourceNotFoundError("/",msg="root url gives 404")
            if resp.status in (401,403):
                raise PermissionDeniedError("listdir (http %s)" % resp.status)
            if resp.status != 207:
                msg = "server at %s doesn't speak WebDAV" % (self.url,)
                raise RemoteConnectionError("",msg=msg,details=resp.read())
        finally:
            resp.close()
        self.url = resp.request_url
        self._url_p = urlparse(self.url)

    def close(self):
        for con in self._connections:
            con.close()
        super(DAVFS,self).close()

    def _take_connection(self,url):
        """Get a connection to the given url's host, re-using if possible."""
        scheme = url.scheme.lower()
        hostname = url.hostname
        port = url.port
        if not port:
            try:
                port = self._DEFAULT_PORT_NUMBERS[scheme]
            except KeyError:
                msg = "unsupported protocol: '%s'" % (url.scheme,)
                raise RemoteConnectionError(msg=msg)
        #  Can we re-use an existing connection?
        with self._connection_lock:
            now = time.time()
            try:
                free_connections = self._free_connections[(hostname,port)]
            except KeyError:
                self._free_connections[(hostname,port)] = deque()
                free_connections = self._free_connections[(hostname,port)]
            else:
                while free_connections:
                    (when,con) = free_connections.popleft()
                    if when + 30 > now:
                        return (False,con)
                    self._discard_connection(con)
        #  Nope, we need to make a fresh one.
        try:
            ConClass = self.connection_classes[scheme]
        except KeyError:
            msg = "unsupported protocol: '%s'" % (url.scheme,)
            raise RemoteConnectionError(msg=msg)
        con = ConClass(url.hostname,url.port,timeout=self.timeout)
        self._connections.append(con)
        return (True,con)

    def _give_connection(self,url,con):
        """Return a connection to the pool, or destroy it if dead."""
        scheme = url.scheme.lower()
        hostname = url.hostname
        port = url.port
        if not port:
            try:
                port = self._DEFAULT_PORT_NUMBERS[scheme]
            except KeyError:
                msg = "unsupported protocol: '%s'" % (url.scheme,)
                raise RemoteConnectionError(msg=msg)
        with self._connection_lock:
            now = time.time()
            try:
                free_connections = self._free_connections[(hostname,port)]
            except KeyError:
                self._free_connections[(hostname,port)] = deque()
                free_connections = self._free_connections[(hostname,port)]
            free_connections.append((now,con))

    def _discard_connection(self,con):
        con.close()
        self._connections.remove(con)

    def __str__(self):
        return '<DAVFS: %s>' % (self.url,)
    __repr__ = __str__

    def __getstate__(self):
        state = super(DAVFS,self).__getstate__()
        del state["_connection_lock"]
        del state["_connections"]
        del state["_free_connections"]
        # Python2.5 cannot load pickled urlparse.ParseResult objects.
        del state["_url_p"]
        # CookieJar objects contain a lock, so they can't be pickled.
        del state["_cookiejar"]
        return state

    def __setstate__(self,state):
        super(DAVFS,self).__setstate__(state)
        self._connections = []
        self._free_connections = {}
        self._connection_lock = threading.Lock()
        self._url_p = urlparse(self.url)
        self._cookiejar = cookielib.CookieJar()

    def getpathurl(self, path, allow_none=False):
        """Convert a client-side path into a server-side URL."""
        path = relpath(normpath(path))
        if path.endswith("/"):
            path = path[:-1]
        if isinstance(path,unicode):
            path = path.encode("utf8")
        return self.url + urlquote(path)

    def _url2path(self,url):
        """Convert a server-side URL into a client-side path."""
        path = urlunquote(urlparse(url).path)
        root = urlunquote(self._url_p.path)
        path = path[len(root)-1:].decode("utf8")
        while path.endswith("/"):
            path = path[:-1]
        return path

    def _isurl(self,path,url):
        """Check whether the given URL corresponds to the given local path."""
        path = normpath(relpath(path))
        upath = relpath(normpath(self._url2path(url)))
        return path == upath

    def _request(self,path,method,body="",headers={}):
        """Issue a HTTP request to the remote server.

        This is a simple wrapper around httplib that does basic error and
        sanity checking e.g. following redirects and providing authentication.
        """
        url = self.getpathurl(path)
        visited = []
        resp = None
        try:
            resp = self._raw_request(url,method,body,headers)
            #  Loop to retry for redirects and authentication responses.
            while resp.status in (301,302,401,403):
                resp.close()
                if resp.status in (301,302,):
                    visited.append(url)
                    url = resp.getheader("Location",None)
                    if not url:
                        raise OperationFailedError(msg="no location header in 301 response")
                    if url in visited:
                        raise OperationFailedError(msg="redirection seems to be looping")
                    if len(visited) > 10:
                        raise OperationFailedError("too much redirection")
                elif resp.status in (401,403):
                    if self.get_credentials is None:
                        break
                    else:
                        creds = self.get_credentials(self.credentials)
                        if creds is None:
                            break
                        else:
                            self.credentials = creds
                resp = self._raw_request(url,method,body,headers)
        except Exception:
            if resp is not None:
                resp.close()
            raise
        resp.request_url = url
        return resp

    def _raw_request(self,url,method,body,headers,num_tries=0):
        """Perform a single HTTP request, without any error handling."""
        if self.closed:
            raise RemoteConnectionError("",msg="FS is closed")
        if isinstance(url,basestring):
            url = urlparse(url)
        if self.credentials is not None:
            username = self.credentials.get("username","")
            password = self.credentials.get("password","")
            if username is not None and password is not None:
                creds = "%s:%s" % (username,password,)
                creds = "Basic %s" % (base64.b64encode(creds).strip(),)
                headers["Authorization"] = creds
        (size,chunks) = normalize_req_body(body)
        try:
            (fresh,con) = self._take_connection(url)
            try:
                con.putrequest(method,url.path)
                if size is not None:
                    con.putheader("Content-Length",str(size))
                if hasattr(body,"md5"):
                    md5 = body.md5.decode("hex").encode("base64")
                    con.putheader("Content-MD5",md5)
                for hdr,val in headers.iteritems():
                    con.putheader(hdr,val)
                self._cookiejar.add_cookie_header(FakeReq(con,url.scheme,url.path))
                con.endheaders()
                for chunk in chunks:
                    con.send(chunk)
                    if self.closed:
                        raise RemoteConnectionError("",msg="FS is closed")
                resp = con.getresponse()
                self._cookiejar.extract_cookies(FakeResp(resp),FakeReq(con,url.scheme,url.path))
            except Exception:
                self._discard_connection(con)
                raise
            else:
                old_close = resp.close
                def new_close():
                    del resp.close
                    old_close()
                    con.close()
                    self._give_connection(url,con)
                resp.close = new_close
                return resp
        except socket.error, e:
            if not fresh:
                return self._raw_request(url,method,body,headers,num_tries)
            if e.args[0] in _RETRYABLE_ERRORS:
                if num_tries < 3:
                    num_tries += 1
                    return self._raw_request(url,method,body,headers,num_tries)
            try:
                msg = e.args[1]
            except IndexError:
                msg = str(e)
            raise RemoteConnectionError("",msg=msg,details=e)

    def setcontents(self,path, data=b'', encoding=None, errors=None, chunk_size=1024 * 64):
        if isinstance(data, six.text_type):
            data = data.encode(encoding=encoding, errors=errors)
        resp = self._request(path, "PUT", data)
        resp.close()
        if resp.status == 405:
            raise ResourceInvalidError(path)
        if resp.status == 409:
            raise ParentDirectoryMissingError(path)
        if resp.status not in (200,201,204):
            raise_generic_error(resp,"setcontents",path)

    @iotools.filelike_to_stream
    def open(self,path,mode="r", **kwargs):
        mode = mode.replace("b","").replace("t","")
        # Truncate the file if requested
        contents = b("")
        if "w" in mode:
            self.setcontents(path,contents)
        else:
            contents = self._request(path,"GET")
            if contents.status == 404:
                # Create the file if it's missing in append mode.
                if "a" not in mode:
                    contents.close()
                    raise ResourceNotFoundError(path)
                contents = b("")
                self.setcontents(path,contents)
            elif contents.status in (401,403):
                contents.close()
                raise PermissionDeniedError("open")
            elif contents.status != 200:
                contents.close()
                raise_generic_error(contents,"open",path)
            elif self.isdir(path):
                contents.close()
                raise ResourceInvalidError(path)
        #  For streaming reads, return the socket contents directly.
        if mode == "r-":
            contents.size = contents.getheader("Content-Length",None)
            if contents.size is not None:
                try:
                    contents.size = int(contents.size)
                except ValueError:
                    contents.size = None
            if not hasattr(contents,"__exit__"):
                contents.__enter__ = lambda *a: contents
                contents.__exit__ = lambda *a: contents.close()
            return contents
        #  For everything else, use a RemoteFileBuffer.
        #  This will take care of closing the socket when it's done.
        return RemoteFileBuffer(self,path,mode,contents)

    def exists(self,path):
        pf = propfind(prop="<prop xmlns='DAV:'><resourcetype /></prop>")
        response = self._request(path,"PROPFIND",pf.render(),{"Depth":"0"})
        response.close()
        if response.status == 207:
            return True
        if response.status == 404:
            return False
        raise_generic_error(response,"exists",path)

    def isdir(self,path):
        pf = propfind(prop="<prop xmlns='DAV:'><resourcetype /></prop>")
        response = self._request(path,"PROPFIND",pf.render(),{"Depth":"0"})
        try:
            if response.status == 404:
                return False
            if response.status != 207:
                raise_generic_error(response,"isdir",path)
            body = response.read()
            msres = multistatus.parse(body)
            for res in msres.responses:
                if self._isurl(path,res.href):
                   for ps in res.propstats:
                       if ps.props.getElementsByTagNameNS("DAV:","collection"):
                           return True
            return False
        finally:
            response.close()

    def isfile(self,path):
        pf = propfind(prop="<prop xmlns='DAV:'><resourcetype /></prop>")
        response = self._request(path,"PROPFIND",pf.render(),{"Depth":"0"})
        try:
            if response.status == 404:
                return False
            if response.status != 207:
                raise_generic_error(response,"isfile",path)
            msres = multistatus.parse(response.read())
            for res in msres.responses:
               if self._isurl(path,res.href):
                  for ps in res.propstats:
                     rt = ps.props.getElementsByTagNameNS("DAV:","resourcetype")
                     cl = ps.props.getElementsByTagNameNS("DAV:","collection")
                     if rt and not cl:
                        return True
            return False
        finally:
            response.close()

    def listdir(self,path="./",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        return list(self.ilistdir(path=path,wildcard=wildcard,full=full,absolute=absolute,dirs_only=dirs_only,files_only=files_only))

    def ilistdir(self,path="./",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        props = "<D:resourcetype />"
        dir_ok = False
        for res in self._do_propfind(path,props):
            if self._isurl(path,res.href):
               # The directory itself, check it's actually a directory
               for ps in res.propstats:
                   if ps.props.getElementsByTagNameNS("DAV:","collection"):
                      dir_ok = True
                      break
            else:
                nm = basename(self._url2path(res.href))
                entry_ok = False
                if dirs_only:
                    for ps in res.propstats:
                        if ps.props.getElementsByTagNameNS("DAV:","collection"):
                            entry_ok = True
                            break
                elif files_only:
                    for ps in res.propstats:
                        if ps.props.getElementsByTagNameNS("DAV:","collection"):
                            break
                    else:
                        entry_ok = True
                else:
                    entry_ok = True
                if not entry_ok:
                    continue
                if wildcard is not None:
                    if isinstance(wildcard,basestring):
                        if not fnmatch.fnmatch(nm,wildcard):
                            continue
                    else:
                        if not wildcard(nm):
                            continue
                if full:
                    yield relpath(pathjoin(path,nm))
                elif absolute:
                    yield abspath(pathjoin(path,nm))
                else:
                    yield nm
        if not dir_ok:
            raise ResourceInvalidError(path)

    def listdirinfo(self,path="./",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        return list(self.ilistdirinfo(path=path,wildcard=wildcard,full=full,absolute=absolute,dirs_only=dirs_only,files_only=files_only))

    def ilistdirinfo(self,path="./",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        props = "<D:resourcetype /><D:getcontentlength />" \
                "<D:getlastmodified /><D:getetag />"
        dir_ok = False
        for res in self._do_propfind(path,props):
            if self._isurl(path,res.href):
               # The directory itself, check it's actually a directory
               for ps in res.propstats:
                   if ps.props.getElementsByTagNameNS("DAV:","collection"):
                      dir_ok = True
                      break
            else:
                # An entry in the directory, check if it's of the
                # appropriate type and add to entries list as required.
                info = self._info_from_propfind(res)
                nm = basename(self._url2path(res.href))
                entry_ok = False
                if dirs_only:
                    for ps in res.propstats:
                        if ps.props.getElementsByTagNameNS("DAV:","collection"):
                            entry_ok = True
                            break
                elif files_only:
                    for ps in res.propstats:
                        if ps.props.getElementsByTagNameNS("DAV:","collection"):
                            break
                    else:
                        entry_ok = True
                else:
                    entry_ok = True
                if not entry_ok:
                    continue
                if wildcard is not None:
                    if isinstance(wildcard,basestring):
                        if not fnmatch.fnmatch(nm,wildcard):
                            continue
                    else:
                        if not wildcard(nm):
                            continue
                if full:
                    yield (relpath(pathjoin(path,nm)),info)
                elif absolute:
                    yield (abspath(pathjoin(path,nm)),info)
                else:
                    yield (nm,info)
        if not dir_ok:
            raise ResourceInvalidError(path)

    def makedir(self,path,recursive=False,allow_recreate=False):
        response = self._request(path,"MKCOL")
        response.close()
        if response.status == 201:
            return True
        if response.status == 409:
            if not recursive:
                raise ParentDirectoryMissingError(path)
            self.makedir(dirname(path),recursive=True,allow_recreate=True)
            self.makedir(path,recursive=False,allow_recreate=allow_recreate)
            return True
        if response.status == 405:
            if not self.isdir(path):
                raise ResourceInvalidError(path)
            if not allow_recreate:
                raise DestinationExistsError(path)
            return True
        if response.status < 200 or response.status >= 300:
            raise_generic_error(response,"makedir",path)

    def remove(self,path):
        if self.isdir(path):
            raise ResourceInvalidError(path)
        response = self._request(path,"DELETE")
        response.close()
        if response.status == 405:
            raise ResourceInvalidError(path)
        if response.status < 200 or response.status >= 300:
            raise_generic_error(response,"remove",path)
        return True

    def removedir(self,path,recursive=False,force=False):
        if self.isfile(path):
            raise ResourceInvalidError(path)
        if not force and self.listdir(path):
            raise DirectoryNotEmptyError(path)
        response = self._request(path,"DELETE")
        response.close()
        if response.status == 405:
            raise ResourceInvalidError(path)
        if response.status < 200 or response.status >= 300:
            raise_generic_error(response,"removedir",path)
        if recursive and path not in ("","/"):
            try:
                self.removedir(dirname(path),recursive=True)
            except DirectoryNotEmptyError:
                pass
        return True

    def rename(self,src,dst):
        self._move(src,dst)

    def getinfo(self,path):
        info = {}
        info["name"] = basename(path)
        pf = propfind(prop="<prop xmlns='DAV:'><resourcetype /><getcontentlength /><getlastmodified /><getetag /></prop>")
        response = self._request(path,"PROPFIND",pf.render(),{"Depth":"0"})
        try:
            if response.status != 207:
                raise_generic_error(response,"getinfo",path)
            msres = multistatus.parse(response.read())
            for res in msres.responses:
                if self._isurl(path,res.href):
                    info.update(self._info_from_propfind(res))
            if "st_mode" not in info:
               info["st_mode"] = 0700 | statinfo.S_IFREG
            return info
        finally:
            response.close()

    def _do_propfind(self,path,props):
        """Incremental PROPFIND parsing, for use with ilistdir/ilistdirinfo.

        This generator method incrementally parses the results returned by
        a PROPFIND, yielding each <response> object as it becomes available.
        If the server is able to send responses in chunked encoding, then
        this can substantially speed up iterating over the results.
        """
        pf = propfind(prop="<D:prop xmlns:D='DAV:'>"+props+"</D:prop>")
        response = self._request(path,"PROPFIND",pf.render(),{"Depth":"1"})
        try:
            if response.status == 404:
                raise ResourceNotFoundError(path)
            if response.status != 207:
                raise_generic_error(response,"listdir",path)
            xmlevents = xml.dom.pulldom.parse(response,bufsize=1024)
            for (evt,node) in xmlevents:
                if evt == xml.dom.pulldom.START_ELEMENT:
                    if node.namespaceURI == "DAV:":
                        if node.localName == "response":
                            xmlevents.expandNode(node)
                            yield xmlobj.response.parse(node)
        finally:
            response.close()

    def _info_from_propfind(self,res):
        info = {}
        for ps in res.propstats:
            findElements = ps.props.getElementsByTagNameNS
            # TODO: should check for status of the propfind first...
            # check for directory indicator
            if findElements("DAV:","collection"):
                info["st_mode"] = 0700 | statinfo.S_IFDIR
            # check for content length
            cl = findElements("DAV:","getcontentlength")
            if cl:
                cl = "".join(c.nodeValue for c in cl[0].childNodes)
                try:
                    info["size"] = int(cl)
                except ValueError:
                    pass
            # check for last modified time
            lm = findElements("DAV:","getlastmodified")
            if lm:
                lm = "".join(c.nodeValue for c in lm[0].childNodes)
                try:
                    # TODO: more robust datetime parsing
                    fmt = "%a, %d %b %Y %H:%M:%S GMT"
                    mtime = datetime.datetime.strptime(lm,fmt)
                    info["modified_time"] = mtime
                except ValueError:
                    pass
            # check for etag
            etag = findElements("DAV:","getetag")
            if etag:
                etag = "".join(c.nodeValue for c in etag[0].childNodes)
                if etag:
                    info["etag"] = etag
        if "st_mode" not in info:
            info["st_mode"] = 0700 | statinfo.S_IFREG
        return info


    def copy(self,src,dst,overwrite=False,chunk_size=None):
        if self.isdir(src):
            msg = "Source is not a file: %(path)s"
            raise ResourceInvalidError(src, msg=msg)
        self._copy(src,dst,overwrite=overwrite)

    def copydir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=0):
        if self.isfile(src):
            msg = "Source is not a directory: %(path)s"
            raise ResourceInvalidError(src, msg=msg)
        self._copy(src,dst,overwrite=overwrite)

    def _copy(self,src,dst,overwrite=False):
        headers = {"Destination":self.getpathurl(dst)}
        if overwrite:
            headers["Overwrite"] = "T"
        else:
            headers["Overwrite"] = "F"
        response = self._request(src,"COPY",headers=headers)
        response.close()
        if response.status == 412:
            raise DestinationExistsError(dst)
        if response.status == 409:
            raise ParentDirectoryMissingError(dst)
        if response.status < 200 or response.status >= 300:
            raise_generic_error(response,"copy",src)

    def move(self,src,dst,overwrite=False,chunk_size=None):
        if self.isdir(src):
            msg = "Source is not a file: %(path)s"
            raise ResourceInvalidError(src, msg=msg)
        self._move(src,dst,overwrite=overwrite)

    def movedir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=0):
        if self.isfile(src):
            msg = "Source is not a directory: %(path)s"
            raise ResourceInvalidError(src, msg=msg)
        self._move(src,dst,overwrite=overwrite)

    def _move(self,src,dst,overwrite=False):
        headers = {"Destination":self.getpathurl(dst)}
        if overwrite:
            headers["Overwrite"] = "T"
        else:
            headers["Overwrite"] = "F"
        response = self._request(src,"MOVE",headers=headers)
        response.close()
        if response.status == 412:
            raise DestinationExistsError(dst)
        if response.status == 409:
            raise ParentDirectoryMissingError(dst)
        if response.status < 200 or response.status >= 300:
            raise_generic_error(response,"move",src)

    @staticmethod
    def _split_xattr(name):
        """Split extended attribute name into (namespace,localName) pair."""
        idx = len(name)-1
        while idx >= 0 and name[idx].isalnum():
            idx -= 1
        return (name[:idx+1],name[idx+1:])

    def getxattr(self,path,name,default=None):
        (namespaceURI,localName) = self._split_xattr(name)
        # TODO: encode xml character entities in the namespace
        if namespaceURI:
            pf = propfind(prop="<D:prop xmlns:D='DAV:' xmlns='"+namespaceURI+"'><"+localName+" /></D:prop>")
        else:
            pf = propfind(prop="<D:prop xmlns:D='DAV:'><"+localName+" /></D:prop>")
        response = self._request(path,"PROPFIND",pf.render(),{"Depth":"0"})
        try:
            if response.status != 207:
                raise_generic_error(response,"getxattr",path)
            msres = multistatus.parse(response.read())
        finally:
            response.close()
        for res in msres.responses:
            if self._isurl(path,res.href):
               for ps in res.propstats:
                   if namespaceURI:
                       findElements = ps.props.getElementsByTagNameNS
                       propNode = findElements(namespaceURI,localName)
                   else:
                       findElements = ps.props.getElementsByTagName
                       propNode = findElements(localName)
                   if propNode:
                       propNode = propNode[0]
                       if ps.status.code == 200:
                         return "".join(c.toxml() for c in propNode.childNodes)
                       if ps.status.code == 404:
                         return default
                   raise OperationFailedError("getxattr",msres.render())
        return default

    def setxattr(self,path,name,value):
        (namespaceURI,localName) = self._split_xattr(name)
        # TODO: encode xml character entities in the namespace
        if namespaceURI:
            p = "<%s xmlns='%s'>%s</%s>" % (localName,namespaceURI,value,localName)
        else:
            p = "<%s>%s</%s>" % (localName,value,localName)
        pu = propertyupdate()
        pu.commands.append(set(props="<D:prop xmlns:D='DAV:'>"+p+"</D:prop>"))
        response = self._request(path,"PROPPATCH",pu.render(),{"Depth":"0"})
        response.close()
        if response.status < 200 or response.status >= 300:
            raise_generic_error(response,"setxattr",path)

    def delxattr(self,path,name):
        (namespaceURI,localName) = self._split_xattr(name)
        # TODO: encode xml character entities in the namespace
        if namespaceURI:
            p = "<%s xmlns='%s' />" % (localName,namespaceURI,)
        else:
            p = "<%s />" % (localName,)
        pu = propertyupdate()
        pu.commands.append(remove(props="<D:prop xmlns:D='DAV:'>"+p+"</D:prop>"))
        response = self._request(path,"PROPPATCH",pu.render(),{"Depth":"0"})
        response.close()
        if response.status < 200 or response.status >= 300:
            raise_generic_error(response,"delxattr",path)

    def listxattrs(self,path):
        pf = propfind(propname=True)
        response = self._request(path,"PROPFIND",pf.render(),{"Depth":"0"})
        try:
            if response.status != 207:
                raise_generic_error(response,"listxattrs",path)
            msres = multistatus.parse(response.read())
        finally:
            response.close()
        props = []
        for res in msres.responses:
            if self._isurl(path,res.href):
               for ps in res.propstats:
                   for node in ps.props.childNodes:
                       if node.nodeType != node.ELEMENT_NODE:
                           continue
                       if node.namespaceURI:
                           if node.namespaceURI in ("DAV:","PYFS:",):
                               continue
                           propname = node.namespaceURI + node.localName
                       else:
                           propname = node.nodeName
                       props.append(propname)
        return props

    # TODO: bulk getxattrs() and setxattrs() methods



def raise_generic_error(response,opname,path):
    if response.status == 404:
        raise ResourceNotFoundError(path,details=response.read())
    if response.status in (401,403):
        raise PermissionDeniedError(opname,details=response.read())
    if response.status == 423:
        raise ResourceLockedError(path,opname=opname,details=response.read())
    if response.status == 501:
        raise UnsupportedError(opname,details=response.read())
    if response.status == 405:
        raise ResourceInvalidError(path,opname=opname,details=response.read())
    raise OperationFailedError(opname,msg="Server Error: %s" % (response.status,),details=response.read())

