#  Copyright (c) 2009-2010, Cloud Matrix Pty. Ltd.
#  All rights reserved; available under the terms of the MIT License.
"""

    fs.contrib.davfs.util:  utils for FS WebDAV implementation.

"""

import os
import re
import cookielib


def get_fileno(file):
    """Get the os-level fileno of a file-like object.

    This function decodes several common file wrapper structures in an attempt
    to determine the underlying OS-level fileno for an object.
    """
    while not hasattr(file,"fileno"):
        if hasattr(file,"file"):
            file = file.file
        elif hasattr(file,"_file"):
            file = file._file
        elif hasattr(file,"_fileobj"):
            file = file._fileobj
        else:
           raise AttributeError
    return file.fileno()


def get_filesize(file):
    """Get the "size" attribute of a file-like object."""
    while not hasattr(file,"size"):
        if hasattr(file,"file"):
            file = file.file
        elif hasattr(file,"_file"):
            file = file._file
        elif hasattr(file,"_fileobj"):
            file = file._fileobj
        else:
           raise AttributeError
    return file.size

    
def file_chunks(f,chunk_size=1024*64):
    """Generator yielding chunks of a file.

    This provides a simple way to iterate through binary data chunks from
    a file.  Recall that using a file directly as an iterator generates the
    *lines* from that file, which is useless and very inefficient for binary
    data.
    """
    chunk = f.read(chunk_size)
    while chunk:
        yield chunk
        chunk = f.read(chunk_size)


def normalize_req_body(body,chunk_size=1024*64):
    """Convert given request body into (size,data_iter) pair.

    This function is used to accept a variety of different inputs in HTTP
    requests, converting them to a standard format.
    """
    if hasattr(body,"getvalue"):
        value = body.getvalue()
        return (len(value),[value])
    elif hasattr(body,"read"):
        try:
            size = int(get_filesize(body))
        except (AttributeError,TypeError):
            try:
                size = os.fstat(get_fileno(body)).st_size
            except (AttributeError,OSError):
                size = None
        return (size,file_chunks(body,chunk_size))
    else:
        body = str(body)
        return (len(body),[body])


class FakeReq:
    """Compatability interface to use cookielib with raw httplib objects."""

    def __init__(self,connection,scheme,path):
        self.connection = connection
        self.scheme = scheme
        self.path = path

    def get_full_url(self):
        return self.scheme + "://" + self.connection.host + self.path

    def get_type(self):
        return self.scheme

    def get_host(self):
        return self.connection.host

    def is_unverifiable(self):
        return True

    def get_origin_req_host(self):
        return self.connection.host

    def has_header(self,header):
        return False

    def add_unredirected_header(self,header,value):
        self.connection.putheader(header,value)
    

class FakeResp:
    """Compatability interface to use cookielib with raw httplib objects."""

    def __init__(self,response):
        self.response = response

    def info(self):
        return self

    def getheaders(self,header):
        header = header.lower()
        headers = self.response.getheaders()
        return [v for (h,v) in headers if h.lower() == header]


#  The standard cooklielib cookie parser doesn't seem to handle multiple
#  cookies correctory, so we replace it with a better version.  This code
#  is a tweaked version of the cookielib function of the same name.
#
_test_cookie = "sessionid=e9c9b002befa93bd865ce155270307ef; Domain=.cloud.me; expires=Wed, 10-Feb-2010 03:27:20 GMT; httponly; Max-Age=1209600; Path=/, sessionid_https=None; Domain=.cloud.me; expires=Wed, 10-Feb-2010 03:27:20 GMT; httponly; Max-Age=1209600; Path=/; secure"
if len(cookielib.parse_ns_headers([_test_cookie])) != 2:
    def parse_ns_headers(ns_headers):
      """Improved parser for netscape-style cookies.

      This version can handle multiple cookies in a single header.
      """
      known_attrs = ("expires", "domain", "path", "secure","port", "max-age")
      result = []
      for ns_header in ns_headers:
        pairs = []
        version_set = False
        for ii, param in enumerate(re.split(r"(;\s)|(,\s(?=[a-zA-Z0-9_\-]+=))", ns_header)):
            if param is None:
                continue
            param = param.rstrip()
            if param == "" or param[0] == ";":
                continue
            if param[0] == ",":
                if pairs:
                    if not version_set:
                        pairs.append(("version", "0"))
                    result.append(pairs)
                pairs = []
                continue
            if "=" not in param:
                k, v = param, None
            else:
                k, v = re.split(r"\s*=\s*", param, 1)
                k = k.lstrip()
            if ii != 0:
                lc = k.lower()
                if lc in known_attrs:
                    k = lc
                if k == "version":
                    # This is an RFC 2109 cookie.
                    version_set = True
                if k == "expires":
                    # convert expires date to seconds since epoch
                    if v.startswith('"'): v = v[1:]
                    if v.endswith('"'): v = v[:-1]
                    v = cookielib.http2time(v)  # None if invalid
            pairs.append((k, v))
        if pairs:
            if not version_set:
                pairs.append(("version", "0"))
            result.append(pairs)
      return result
    cookielib.parse_ns_headers = parse_ns_headers
    assert len(cookielib.parse_ns_headers([_test_cookie])) == 2 

