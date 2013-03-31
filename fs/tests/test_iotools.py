from __future__ import unicode_literals

from fs import iotools

import io
import unittest
from os.path import dirname, join, abspath

try:
    unicode
except NameError:
    unicode = str


class OpenFilelike(object):
    def __init__(self, make_f):
        self.make_f = make_f

    @iotools.filelike_to_stream
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        return self.make_f()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.f.close()


class TestIOTools(unittest.TestCase):

    def get_bin_file(self):
        path = join(dirname(abspath(__file__)), 'data/UTF-8-demo.txt')
        return io.open(path, 'rb')

    def test_make_stream(self):
        """Test make_stream"""
        with self.get_bin_file() as f:
            text = f.read()
            self.assert_(isinstance(text, bytes))

        with self.get_bin_file() as f:
            with iotools.make_stream("data/UTF-8-demo.txt", f, 'rt') as f2:
                text = f2.read()
                self.assert_(isinstance(text, unicode))

    def test_decorator(self):
        """Test filelike_to_stream decorator"""
        o = OpenFilelike(self.get_bin_file)
        with o.open('file', 'rb') as f:
            text = f.read()
            self.assert_(isinstance(text, bytes))

        with o.open('file', 'rt') as f:
            text = f.read()
            self.assert_(isinstance(text, unicode))
