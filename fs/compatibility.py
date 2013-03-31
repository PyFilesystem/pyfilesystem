"""
Some functions for Python3 compatibility.

Not for general usage, the functionality in this file is exposed elsewhere

"""

import six
from six import PY3


def copy_file_to_fs(data, dst_fs, dst_path, chunk_size=64 * 1024, progress_callback=None, finished_callback=None):
    """Copy data from a string or a file-like object to a given fs/path"""
    if progress_callback is None:
        progress_callback = lambda bytes_written: None
    bytes_written = 0
    f = None
    try:
        progress_callback(bytes_written)
        if hasattr(data, "read"):
            read = data.read
            chunk = read(chunk_size)
            if isinstance(chunk, six.text_type):
                f = dst_fs.open(dst_path, 'w')
            else:
                f = dst_fs.open(dst_path, 'wb')
            write = f.write
            while chunk:
                write(chunk)
                bytes_written += len(chunk)
                progress_callback(bytes_written)
                chunk = read(chunk_size)
        else:
            if isinstance(data, six.text_type):
                f = dst_fs.open(dst_path, 'w')
            else:
                f = dst_fs.open(dst_path, 'wb')
            f.write(data)
            bytes_written += len(data)
            progress_callback(bytes_written)

        if hasattr(f, 'flush'):
            f.flush()
        if finished_callback is not None:
            finished_callback()

    finally:
        if f is not None:
            f.close()
