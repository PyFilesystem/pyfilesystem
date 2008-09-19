"""Contains a number of high level utility functions for working with FS objects."""

import shutil
from mountfs import MountFS

def copyfile(src_fs, src_path, dst_fs, dst_path, chunk_size=1024*16):
    """Copy a file from one filesystem to another. Will use system copyfile, if both files have a syspath.
    Otherwise file will be copied a chunk at a time.

    src_fs -- Source filesystem object
    src_path -- Source path
    dst_fs -- Destination filesystem object
    dst_path -- Destination filesystem object
    chunk_size -- Size of chunks to move if system copyfile is not available (default 16K)

    """
    src_syspath = src_fs.getsyspath(src_path, default="")
    dst_syspath = dst_fs.getsyspath(dst_path, default="")

    # System copy if there are two sys paths
    if src_syspath and dst_syspath:
        shutil.copyfile(src_syspath, dst_syspath)
        return

    src, dst = None

    try:
        # Chunk copy
        src = src_fs.open(src_path, 'rb')
        dst = dst_fs.open(dst_path, 'wb')

        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dst.write(chunk)

    finally:
        if src is not None:
            src.close()
        if dst is not None:
            dst.close()


def movefile(src_fs, src_path, dst_fs, dst_path, chunk_size=1024*16):

    """Move a file from one filesystem to another. Will use system copyfile, if both files have a syspath.
    Otherwise file will be copied a chunk at a time.

    src_fs -- Source filesystem object
    src_path -- Source path
    dst_fs -- Destination filesystem object
    dst_path -- Destination filesystem object
    chunk_size -- Size of chunks to move if system copyfile is not available (default 16K)

    """

    src_syspath = src_fs.getsyspath(src_path, default="")
    dst_syspath = dst_fs.getsyspath(dst_path, default="")

    # System copy if there are two sys paths
    if src_syspath and dst_syspath:
        shutil.movefile(src_syspath, dst_syspath)
        return

    src, dst = None

    try:
        # Chunk copy
        src = src_fs.open(src_path, 'rb')
        dst = dst_fs.open(dst_path, 'wb')

        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dst.write(chunk)

        src_fs.remove(src)

    finally:
        if src is not None:
            src.close()
        if dst is not None:
            dst.close()

def movedir(fs1, fs2, ignore_errors=False, chunk_size=16384):
    """Moves contents of a directory from one filesystem to another.

    fs1 -- Source filesystem, or a tuple of (<filesystem>, <directory path>)
    fs2 -- Destination filesystem, or a tuple of (<filesystem>, <directory path>)
    ignore_errors -- If True, exceptions from file moves are ignored
    chunk_size -- Size of chunks to move if a simple copy is used

    """
    if isinstance(fs1, tuple):
        fs1, dir1 = fs1
        fs1 = fs1.opendir(dir1)
    if isinstance(fs2, tuple):
        fs2, dir2 = fs2
        fs2 = fs2.opendir(dir2)

    mount_fs = MountFS()
    mount_fs.mount('dir1', fs1)
    mount_fs.mount('dir2', fs2)
    mount_fs.movedir('dir1', 'dir2', ignore_errors=ignore_errors, chunk_size=chunk_size)

def copydir(fs1, fs2, ignore_errors=False, chunk_size=16384):
    """Copies contents of a directory from one filesystem to another.

    fs1 -- Source filesystem, or a tuple of (<filesystem>, <directory path>)
    fs2 -- Destination filesystem, or a tuple of (<filesystem>, <directory path>)
    ignore_errors -- If True, exceptions from file moves are ignored
    chunk_size -- Size of chunks to move if a simple copy is used

    """
    if isinstance(fs1, tuple):
        fs1, dir1 = fs1
        fs1 = fs1.opendir(dir1)
    if isinstance(fs2, tuple):
        fs2, dir2 = fs2
        fs2 = fs2.opendir(dir2)

    mount_fs = MountFS()
    mount_fs.mount('dir1', fs1)
    mount_fs.mount('dir2', fs2)
    mount_fs.movedir('dir1', 'dir2', ignore_errors=ignore_errors, chunk_size=chunk_size)

def countbytes(count_fs):
    """Returns the total number of bytes contained within files in a filesystem.

    count_fs -- A filesystem object

    """
    total = sum(count_fs.getsize(f) for f in count_fs.walkfiles())
