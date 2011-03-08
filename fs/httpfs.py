"""
fs.httpfs
=========


"""

from fs.base import FS
from fs.path import normpath
from fs.errors import ResourceNotFoundError, UnsupportedError
from urllib2 import urlopen, URLError

class HTTPFS(FS):
    
    """Can barely be called a filesystem, because HTTP servers generally don't support 
    typical filesystem functionality. This class exists to allow the :doc:`opener` system
    to read files over HTTP. 
    
    If you do need filesystem like functionality over HTTP, see :mod:`fs.contrib.davfs`.
     
    """
    
    def __init__(self, url):
        """
        
        :param url: The base URL
        
        """
        self.root_url = url
        
    def _make_url(self, path):
        path = normpath(path)
        url = '%s/%s' % (self.root_url.rstrip('/'), path.lstrip('/'))
        return url

    def open(self, path, mode="r"):
        
        if '+' in mode or 'w' in mode or 'a' in mode:
            raise UnsupportedError('write')
        
        url = self._make_url(path)
        try:
            f = urlopen(url)
        except URLError, e:
            raise ResourceNotFoundError(path, details=e)
        except OSError, e:
            raise ResourceNotFoundError(path, details=e)
        
        return f
    
    def exists(self, path):
        return self.isfile(path)
    
    def isdir(self, path):
        return False
    
    def isfile(self, path):
        url = self._make_url(path)
        f = None
        try:
            try:
                f = urlopen(url)
            except (URLError, OSError):
                return False
        finally:
            if f is not None:
                f.close()
            
        return True
    
    def listdir(self, path="./",
                      wildcard=None,
                      full=False,
                      absolute=False,
                      dirs_only=False,
                      files_only=False):
        return []
