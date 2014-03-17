Release Notes
=============

PyFilesystem has reached a point where the interface is relatively stable. The were some backwards incompatibilities introduced with version 0.5.0, due to Python 3 support.

Changes from 0.4.0
------------------

Python 3.X support was added. The interface has remained much the same, but the ``open`` method now works like Python 3's builtin, which handles text encoding more elegantly. i.e. if you open a file in text mode, you get a stream that reads or writes unicode rather than binary strings.

The new signature to the ``open`` method (and ``safeopen``) is as follows::

    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):

In order to keep the same signature across both Python 2 and 3, PyFilesystems uses the ``io`` module from the standard library. Unfortunately this is only available from Python 2.6 onwards, so Python 2.5 support has been dropped. If you need Python 2.5 support, consider sticking to PyFilesystem 0.4.0.

By default the new ``open`` method now returns a unicode text stream, whereas 0.4.0 returned a binary file-like object. If you have code that runs on 0.4.0, you will probably want to either modify your code to work with unicode or explicitly open files in binary mode. The latter is as simple as changing the mode from "r" to "rb" (or "w" to "wb"), but if you were working with unicode, the new text streams will likely save you a few lines of code.

The ``setcontents`` and ``getcontents`` methods have also grown a few parameters in order to work with text files. So you won't require an extra encode / decode step for text files.

