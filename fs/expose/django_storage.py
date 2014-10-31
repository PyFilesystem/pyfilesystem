"""
fs.expose.django
================

Use an FS object for Django File Storage

This module exposes the class "FSStorage", a simple adapter for using FS
objects as Django storage objects.  Simply include the following lines
in your settings.py::

    DEFAULT_FILE_STORAGE = fs.expose.django_storage.FSStorage
    DEFAULT_FILE_STORAGE_FS = OSFS('foo/bar')  # Or whatever FS


"""

from django.conf import settings
from django.core.files.storage import Storage
from django.core.files import File

from fs.path import abspath, dirname
from fs.errors import convert_fs_errors, ResourceNotFoundError

class FSStorage(Storage):
    """Expose an FS object as a Django File Storage object."""

    def __init__(self, fs=None, base_url=None):
        """
        :param fs: an FS object
        :param base_url: The url to prepend to the path
         
        """
        if fs is None:
            fs = settings.DEFAULT_FILE_STORAGE_FS
        if base_url is None:
            base_url = settings.MEDIA_URL
        base_url = base_url.rstrip('/')
        self.fs = fs
        self.base_url = base_url

    def exists(self, name):
        return self.fs.isfile(name)

    def path(self, name):
        path = self.fs.getsyspath(name)
        if path is None:
            raise NotImplementedError
        return path

    @convert_fs_errors
    def size(self, name):
        return self.fs.getsize(name)

    @convert_fs_errors
    def url(self, name):
        return self.base_url + abspath(name)

    @convert_fs_errors
    def _open(self, name, mode):
        return File(self.fs.open(name, mode))

    @convert_fs_errors
    def _save(self, name, content):
        self.fs.makedir(dirname(name), allow_recreate=True, recursive=True)
        self.fs.setcontents(name, content)
        return name

    @convert_fs_errors
    def delete(self, name):
        try:
            self.fs.remove(name)
        except ResourceNotFoundError:
            pass


