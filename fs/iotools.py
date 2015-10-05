from __future__ import unicode_literals
from __future__ import print_function

from fs import SEEK_SET, SEEK_CUR, SEEK_END

import io
from functools import wraps

import six


class RawWrapper(object):
    """Convert a Python 2 style file-like object in to a IO object"""
    def __init__(self, f, mode=None, name=None):
        self._f = f
        self.is_io = isinstance(f, io.IOBase)
        if mode is None and hasattr(f, 'mode'):
            mode = f.mode
        self.mode = mode
        self.name = name
        self.closed = False
        super(RawWrapper, self).__init__()

    def __repr__(self):
        return "<IO wrapper for {0}>".format(self._f)

    def close(self):
        self._f.close()
        self.closed = True

    def fileno(self):
        return self._f.fileno()

    def flush(self):
        return self._f.flush()

    def isatty(self):
        return self._f.isatty()

    def seek(self, offset, whence=SEEK_SET):
        return self._f.seek(offset, whence)

    def readable(self):
        if hasattr(self._f, 'readable'):
            return self._f.readable()
        return 'r' in self.mode

    def writable(self):
        if hasattr(self._f, 'writeable'):
            return self._fs.writeable()
        return 'w' in self.mode

    def seekable(self):
        if hasattr(self._f, 'seekable'):
            return self._f.seekable()
        try:
            self.seek(0, SEEK_CUR)
        except IOError:
            return False
        else:
            return True

    def tell(self):
        return self._f.tell()

    def truncate(self, size=None):
        return self._f.truncate(size)

    def write(self, data):
        if self.is_io:
            return self._f.write(data)
        self._f.write(data)
        return len(data)

    def read(self, n=-1):
        if n == -1:
            return self.readall()
        return self._f.read(n)

    def read1(self, n=-1):
        if self.is_io:
            return self._f.read1(n)
        return self.read(n)

    def readall(self):
        return self._f.read()

    def readinto(self, b):
        if self.is_io:
            return self._f.readinto(b)
        data = self._f.read(len(b))
        bytes_read = len(data)
        b[:len(data)] = data
        return bytes_read

    def readline(self, limit=-1):
        return self._f.readline(limit)

    def readlines(self, hint=-1):
        return self._f.readlines(hint)

    def writelines(self, sequence):
        return self._f.writelines(sequence)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __iter__(self):
        return iter(self._f)


def filelike_to_stream(f):
    @wraps(f)
    def wrapper(self, path, mode='rt', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False, **kwargs):
        file_like = f(self,
                      path,
                      mode=mode,
                      buffering=buffering,
                      encoding=encoding,
                      errors=errors,
                      newline=newline,
                      line_buffering=line_buffering,
                      **kwargs)
        return make_stream(path,
                           file_like,
                           mode=mode,
                           buffering=buffering,
                           encoding=encoding,
                           errors=errors,
                           newline=newline,
                           line_buffering=line_buffering)
    return wrapper


def make_stream(name,
                f,
                mode='r',
                buffering=-1,
                encoding=None,
                errors=None,
                newline=None,
                line_buffering=False,
                **kwargs):
    """Take a Python 2.x binary file and returns an IO Stream"""
    r, w, a, binary = 'r' in mode, 'w' in mode, 'a' in mode, 'b' in mode
    if '+' in mode:
        r, w = True, True

    io_object = RawWrapper(f, mode=mode, name=name)
    if buffering >= 0:
        if r and w:
            io_object = io.BufferedRandom(io_object, buffering or io.DEFAULT_BUFFER_SIZE)
        elif r:
            io_object = io.BufferedReader(io_object, buffering or io.DEFAULT_BUFFER_SIZE)
        elif w:
            io_object = io.BufferedWriter(io_object, buffering or io.DEFAULT_BUFFER_SIZE)

    if not binary:
        io_object = io.TextIOWrapper(io_object,
                                     encoding=encoding or 'utf-8',
                                     errors=errors,
                                     newline=newline,
                                     line_buffering=line_buffering,)

    return io_object


def decode_binary(data, encoding=None, errors=None, newline=None):
    """Decode bytes as though read from a text file"""
    return io.TextIOWrapper(io.BytesIO(data), encoding=encoding or 'utf-8', errors=errors, newline=newline).read()


def make_bytes_io(data, encoding=None, errors=None):
    """Make a bytes IO object from either a string or an open file"""
    if hasattr(data, 'mode') and 'b' in data.mode:
        # It's already a binary file
        return data
    if not isinstance(data, basestring):
        # It's a file, but we don't know if its binary
        # TODO: Is there a better way than reading the entire file?
        data = data.read() or b''
    if isinstance(data, six.text_type):
        # If its text, encoding in to bytes
        data = data.encode(encoding=encoding, errors=errors)
    return io.BytesIO(data)


def copy_file_to_fs(f, fs, path, encoding=None, errors=None, progress_callback=None, chunk_size=64 * 1024):
    """Copy an open file to a path on an FS"""
    if progress_callback is None:
        progress_callback = lambda bytes_written: None
    read = f.read
    chunk = read(chunk_size)
    if isinstance(chunk, six.text_type):
        f = fs.open(path, 'wt', encoding=encoding, errors=errors)
    else:
        f = fs.open(path, 'wb')
    write = f.write
    bytes_written = 0
    try:
        while chunk:
            write(chunk)
            bytes_written += len(chunk)
            progress_callback(bytes_written)
            chunk = read(chunk_size)
    finally:
        f.close()
    return bytes_written


def line_iterator(f, size=None):
    """A not terribly efficient char by char line iterator"""
    read = f.read
    line = []
    append = line.append
    c = 1
    if size is None or size < 0:
        while c:
            c = read(1)
            if c:
                append(c)
            if c in (b'\n', b''):
                yield b''.join(line)
                del line[:]
    else:
        while c:
            c = read(1)
            if c:
                append(c)
            if c in (b'\n', b'') or len(line) >= size:
                yield b''.join(line)
                del line[:]


if __name__ == "__main__":
    print("Reading a binary file")
    bin_file = open('tests/data/UTF-8-demo.txt', 'rb')
    with make_stream('UTF-8-demo.txt', bin_file, 'rb') as f:
        print(repr(f))
        print(type(f.read(200)))

    print("Reading a text file")
    bin_file = open('tests/data/UTF-8-demo.txt', 'rb')
    with make_stream('UTF-8-demo.txt', bin_file, 'rt') as f:
        print(repr(f))
        print(type(f.read(200)))

    print("Reading a buffered binary file")
    bin_file = open('tests/data/UTF-8-demo.txt', 'rb')
    with make_stream('UTF-8-demo.txt', bin_file, 'rb', buffering=0) as f:
        print(repr(f))
        print(type(f.read(200)))
