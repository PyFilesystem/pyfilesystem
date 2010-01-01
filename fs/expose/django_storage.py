"""
fs.expose.django
================

Use an FS object for Django File Storage

"""

from django.conf import settings
from django.core.files.storage import Storage

from fs.path import abspath


class FSStorage(Storage):
    """Expose an FS object as a Django File Storage object."""

    def __init__(self,fs=None,base_url=None):
        if fs is None:
            fs = settings.DEFAULT_FILE_STORAGE_FS
        if base_url is None:
            base_url = settings.MEDIA_URL
        while base_url.endswith("/"):
            base_url = base_url[:-1]
        self.fs = fs
        self.base_url = base_url

    def exists(self,name):
        return self.fs.isfile(name)

    def path(self,name):
        path = self.fs.getsyspath(name)
        if path is None:
            raise NotImplementedError
        return path

    def size(self,name):
        return self.fs.getsize(name)

    def url(self,name):
        return self.base_url + abspath(name)

    def _open(self,name,mode):
        return selfs.fs.open(name,mode)

    def _save(self,name,content):
        self.fs.setcontents(name,content)
        return name

    def delete(self,name):
        try:
            self.fs.remove(name)
        except ResourceNotFoundError:
            pass


