from __future__ import unicode_literals
from __future__ import print_function
import io


class RawWrapper(object):
    """Convert a Python 2 style file-like object in to a IO object"""
    def __init__(self, f, mode=None, name=None):
        self._f = f
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

    def seek(self, offset, whence=io.SEEK_SET):
        return self._f.seek(offset, whence)

    def readable(self):
        return 'r' in self.mode

    def writable(self):
        return 'w' in self.mode

    def seekable(self):
        try:
            self.seek(0, io.SEEK_CUR)
        except IOError:
            return False
        else:
            return True

    def tell(self):
        return self._f.tell()

    def truncate(self, size):
        return self._f.truncate(size)

    def write(self, data):
        return self._f.write(data)

    def read(self, n=-1):
        if n == -1:
            return self.readall()
        return self._f.read(n)

    def read1(self, n=-1):
        return self.read(n)

    def readall(self):
        return self._f.read()

    def readinto(self, b):
        data = self._f.read(len(b))
        bytes_read = len(data)
        b[:len(data)] = data
        return bytes_read

    def write(self, b):
        bytes_written = self._f.write(b)
        return bytes_written

    def writelines(self, sequence):
        return self._f.writelines(sequence)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


def make_stream(name,
                f,
                mode='r',
                buffering=-1,
                encoding=None,
                errors=None,
                newline=None,
                closefd=True,
                line_buffering=False,
                **params):
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
                                     encoding=encoding,
                                     errors=errors,
                                     newline=newline,
                                     line_buffering=line_buffering,)

    return io_object


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
