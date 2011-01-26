import platform
import logging

import fs.errors as errors
from fs import SEEK_END

python3 = int(platform.python_version_tuple()[0]) > 2

if python3:
    from urllib.parse import urlencode, pathname2url, quote
    from urllib.request import Request, urlopen
else:
    from urllib import urlencode, pathname2url
    from urllib2 import Request, urlopen, quote

class PutRequest(Request):
    def __init__(self, *args, **kwargs):
        self.get_method = lambda: u'PUT'
        Request.__init__(self, *args, **kwargs)
         
class DeleteRequest(Request):
    def __init__(self, *args, **kwargs):
        self.get_method = lambda: u'DELETE'
        Request.__init__(self, *args, **kwargs)
              
class Connection:
    def __init__(self, webapi):
        self.webapi = webapi
        self.headers = {'Accept': 'text/plain'}

    def _get_headers(self, f, size=None):
        '''
            Retrieve length of string or file object and prepare HTTP headers.
        '''
        if isinstance(f, basestring):
            # Just set up content length
            size = len(f)
        elif getattr(f, 'read', None):
            if size == None:
                # When size is already known, skip this           
                f.seek(0, SEEK_END)
                size = f.tell()
                f.seek(0)
        else:
            raise errors.UnsupportedError("Cannot handle type %s" % type(f))
        
        headers = {'Content-Length': size}
        headers.update(self.headers)
        return headers

    def _urlencode(self, data):
        _data = {}
        for k, v in data.items():
            _data[k.encode('utf-8')] = v.encode('utf-8')
        return urlencode(_data)

    def _quotepath(self, path, params={}):
        q = quote(path.encode('utf-8'), safe='/')
        if params:
            return u"%s?%s" % (q, self._urlencode(params))
        return q
        
    def _urlopen(self, req):
        try:
            return urlopen(req)
        except Exception, e:
            if not getattr(e, 'getcode', None):
                raise errors.RemoteConnectionError(str(e))
            code = e.getcode()
            if code == 500:
                # Probably out of space or unhappiness error
                raise errors.StorageSpaceError(e.fp.read())
            elif code in (400, 404, 410):
                # Standard not found
                raise errors.ResourceNotFoundError(e.fp.read())
            raise errors.ResourceInvalidError(e.fp.read())
        
    def post(self, path, data={}, params={}):
        data = self._urlencode(data)
        path = self._quotepath(path, params)
        req = Request(''.join([self.webapi, path]), data, headers=self.headers)
        return self._urlopen(req)
    
    def get(self, path, data={}, offset=None, length=None):
        data = self._urlencode(data)
        path = self._quotepath(path)
        if data: 
            path = u'?'.join([path, data])

        headers = {}
        headers.update(self.headers)
        if offset:
            if length:
                headers['Range'] = 'bytes=%d-%d' % \
                                    (int(offset), int(offset+length))
            else:
                headers['Range'] = 'bytes=%d-' % int(offset)
            
        req = Request(''.join([self.webapi, path]), headers=headers)
        return self._urlopen(req)

    def put(self, path, data, size=None, params={}):
        path = self._quotepath(path, params)
        headers = self._get_headers(data, size=size)
        req = PutRequest(''.join([self.webapi, path]), data, headers=headers)    
        return self._urlopen(req)
    
    def delete(self, path, data={}):   
        path = self._quotepath(path)
        req = DeleteRequest(''.join([self.webapi, path]), data, headers=self.headers)
        return self._urlopen(req)
