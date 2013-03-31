"""
fs.httpfs
=========


"""

from fs.base import FS
from fs.path import normpath
from fs.errors import ResourceNotFoundError, UnsupportedError
from fs.filelike import FileWrapper
from fs import iotools

from urllib2 import urlopen, URLError
from datetime import datetime


class HTTPFS(FS):

    """Can barely be called a filesystem, because HTTP servers generally don't support
    typical filesystem functionality. This class exists to allow the :doc:`opener` system
    to read files over HTTP.

    If you do need filesystem like functionality over HTTP, see :mod:`fs.contrib.davfs`.

    """

    _meta = {'read_only': True,
             'network': True}

    def __init__(self, url):
        """

        :param url: The base URL

        """
        self.root_url = url

    def _make_url(self, path):
        path = normpath(path)
        url = '%s/%s' % (self.root_url.rstrip('/'), path.lstrip('/'))
        return url

    @iotools.filelike_to_stream
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):

        if '+' in mode or 'w' in mode or 'a' in mode:
            raise UnsupportedError('write')

        url = self._make_url(path)
        try:
            f = urlopen(url)
        except URLError, e:
            raise ResourceNotFoundError(path, details=e)
        except OSError, e:
            raise ResourceNotFoundError(path, details=e)

        return FileWrapper(f)

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

    def getinfo(self, path):
        url = self._make_url(path)
        info = urlopen(url).info().dict
        if 'content-length' in info:
            info['size'] = info['content-length']
        if 'last-modified' in info:
            info['modified_time'] = datetime.strptime(info['last-modified'],
                                                      "%a, %d %b %Y %H:%M:%S %Z")
        return info
