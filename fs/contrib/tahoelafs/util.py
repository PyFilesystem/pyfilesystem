'''
Created on 25.9.2010

@author: marekp
'''

import sys
import platform
import stat as statinfo

import fs.errors as errors
from fs.path import pathsplit
try:
    # For non-CPython or older CPython versions.
    # Simplejson also comes with C speedup module which
    # is not in standard CPython >=2.6 library.
    import simplejson as json
except ImportError:
    try:
        import json
    except ImportError:
        print "simplejson (http://pypi.python.org/pypi/simplejson/) required"
        raise
    
from .connection import Connection

python3 = int(platform.python_version_tuple()[0]) > 2

if python3:
    from urllib.error import HTTPError
else:
    from urllib2 import HTTPError 
    
class TahoeUtil:
    def __init__(self, webapi):
        self.connection = Connection(webapi)
        
    def createdircap(self):
        return self.connection.post(u'/uri', params={u't': u'mkdir'}).read()
            
    def unlink(self, dircap, path=None):
        path = self.fixwinpath(path, False)
        self.connection.delete(u'/uri/%s%s' % (dircap, path))
    
    def info(self, dircap, path):
        path = self.fixwinpath(path, False)
        meta = json.load(self.connection.get(u'/uri/%s%s' % (dircap, path), {u't': u'json'}))
        return self._info(path, meta)
            
    def fixwinpath(self, path, direction=True):
        '''
            No, Tahoe really does not support file streams...
            This is ugly hack, because it is not Tahoe-specific.
            Should be move to middleware if will be any.
        '''
        if platform.system() != 'Windows':
            return path
        
        if direction and ':' in path:
                path = path.replace(':', '__colon__')
        elif not direction and '__colon__' in path:
                path = path.replace('__colon__', ':')
        return path
    
    def _info(self, path, data):
        if isinstance(data, list):
            type = data[0]
            data = data[1]
        elif isinstance(data, dict):
            type = data['type']
        else:
            raise errors.ResourceInvalidError('Metadata in unknown format!')
        
        if type == 'unknown':
            raise errors.ResourceNotFoundError(path)
        
        info = {'name': unicode(self.fixwinpath(path, True)),
                'type': type,
                'size': data.get('size', 0),
                'ctime': None,
                'uri': data.get('rw_uri', data.get('ro_uri'))}
        if 'metadata' in data:
            info['ctime'] = data['metadata'].get('ctime')
    
        if info['type'] == 'dirnode':
            info['st_mode'] = 0777 |  statinfo.S_IFDIR 
        else:
            info['st_mode'] = 0644

        return info
        
    def list(self, dircap, path=None):
        path = self.fixwinpath(path, False)
        
        data = json.load(self.connection.get(u'/uri/%s%s' % (dircap, path), {u't': u'json'}))     

        if len(data) < 2 or data[0] != 'dirnode':
            raise errors.ResourceInvalidError('Metadata in unknown format!')
        
        data = data[1]['children']
        for i in data.keys():
            x = self._info(i, data[i])
            yield x

    def mkdir(self, dircap, path):
        path = self.fixwinpath(path, False)    
        path = pathsplit(path)
        
        self.connection.post(u"/uri/%s%s" % (dircap, path[0]), data={u't': u'mkdir', u'name': path[1]})
    
    def move(self, dircap, src, dst):
        if src == '/' or dst == '/':
            raise errors.UnsupportedError("Too dangerous operation, aborting")

        src = self.fixwinpath(src, False)
        dst = self.fixwinpath(dst, False)
        
        src_tuple = pathsplit(src)
        dst_tuple = pathsplit(dst)
        
        if src_tuple[0] == dst_tuple[0]:
            # Move inside one directory
            self.connection.post(u"/uri/%s%s" % (dircap, src_tuple[0]), data={u't': u'rename',
                                        u'from_name': src_tuple[1], u'to_name': dst_tuple[1]})
            return

        # Move to different directory. Firstly create link on dst, then remove from src
        try:
            self.info(dircap, dst)
        except errors.ResourceNotFoundError:
            pass
        else:
            self.unlink(dircap, dst)
         
        uri = self.info(dircap, src)['uri']
        self.connection.put(u"/uri/%s%s" % (dircap, dst), data=uri, params={u't': u'uri'})        
        if uri != self.info(dircap, dst)['uri']:
            raise errors.OperationFailedError('Move failed')
        
        self.unlink(dircap, src)
