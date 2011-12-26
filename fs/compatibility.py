"""
Some functions for Python3 compatibility.

Not for general usage, the functionality in this file is exposed elsewhere

"""

import six
from six import PY3

if PY3:
    def copy_file_to_fs(data, dst_fs, dst_path, chunk_size=64 * 1024):
        """Copy data from a string or a file-like object to a given fs/path"""
        if hasattr(data, "read"):       
            read = data.read
            chunk = read(chunk_size)
            f = None
            try:                        
                if isinstance(chunk, six.text_type):
                    f = dst_fs.open(dst_path, 'w')
                else:
                    f = dst_fs.open(dst_path, 'wb')
                    
                write = f.write                
                while chunk:
                    write(chunk)
                    chunk = read(chunk_size)
            finally:
                if f is not None:
                    f.close()
        else:
            f = None
            try:
                if isinstance(data, six.text_type):
                    f = dst_fs.open(dst_path, 'w')
                else:                    
                    f = dst_fs.open(dst_path, 'wb')
                f.write(data)
            finally:
                if f is not None:
                    f.close()
                
else:
    def copy_file_to_fs(data, dst_fs, dst_path, chunk_size=64 * 1024):
        """Copy data from a string or a file-like object to a given fs/path"""       
        f = None
        try:
            f = dst_fs.open(dst_path, 'wb')
            if hasattr(data, "read"):
                read = data.read
                write = f.write
                chunk = read(chunk_size)
                while chunk:
                    write(chunk)
                    chunk = read(chunk_size)
            else:                    
                f.write(data)
            if hasattr(f, 'flush'):
                f.flush()
        finally:
            if f is not None:
                f.close()

