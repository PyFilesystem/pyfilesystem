"""

  fs.utils: high-level utility functions for working with FS objects.

"""

import shutil
from fs.mountfs import MountFS

def copyfile(src_fs, src_path, dst_fs, dst_path, chunk_size=16384):
    """Copy a file from one filesystem to another. Will use system copyfile, if both files have a syspath.
    Otherwise file will be copied a chunk at a time.

    src_fs -- Source filesystem object
    src_path -- Source path
    dst_fs -- Destination filesystem object
    dst_path -- Destination filesystem object
    chunk_size -- Size of chunks to move if system copyfile is not available (default 16K)

    """
    src_syspath = src_fs.getsyspath(src_path) or ""
    dst_syspath = dst_fs.getsyspath(dst_path) or ""

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


def movefile(src_fs, src_path, dst_fs, dst_path, chunk_size=16384):
    """Move a file from one filesystem to another. Will use system copyfile, if both files have a syspath.
    Otherwise file will be copied a chunk at a time.

    src_fs -- Source filesystem object
    src_path -- Source path
    dst_fs -- Destination filesystem object
    dst_path -- Destination filesystem object
    chunk_size -- Size of chunks to move if system copyfile is not available (default 16K)

    """
    src_syspath = src_fs.getsyspath(src_path) or ""
    dst_syspath = dst_fs.getsyspath(dst_path) or ""

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
    mount_fs.copydir('dir1', 'dir2', ignore_errors=ignore_errors, chunk_size=chunk_size)


def countbytes(fs):
    """Returns the total number of bytes contained within files in a filesystem.

    fs -- A filesystem object

    """
    total = sum(fs.getsize(f) for f in fs.walkfiles())
    return total


def find_duplicates(fs, compare_paths=None, quick=False, signature_chunk_size=16*1024, signature_size=10*16*1024):
    """A generator that yields the paths of duplicate files in an FS object.
    Files are considered identical if the contents are the same (dates or
    other attributes not take in to account).

    fs -- A filesystem object
    compare_paths -- An iterable of paths in the FS object, or all files if omited
    quick -- If set to True, the quick method of finding duplicates will be used,
    which can potentially return false positives if the files have the same
    size and start with the same data. Do not use when deleting files!

    signature_chunk_size -- The number of bytes to read before generating a
    signature checksum value
    signature_size -- The total number of bytes read to generate a signature


    """

    from collections import defaultdict
    from zlib import crc32

    if compare_paths is None:
        compare_paths = fs.walkfiles()

    # Create a dictionary that maps file sizes on to the paths of files with
    # that filesize. So we can find files of the same size with a quick lookup
    file_sizes = defaultdict(list)
    for path in compare_paths:
        file_sizes[fs.getsize(path)].append(path)

    size_duplicates = [paths for paths in file_sizes.itervalues() if len(paths) > 1]

    signatures = defaultdict(list)

    # A signature is a tuple of CRC32s for each 4x16K of the file
    # This allows us to find potential duplicates with a dictionary lookup
    for paths in size_duplicates:
        for path in paths:
            signature = []
            fread = None
            bytes_read = 0
            try:
                fread = fs.open(path, 'rb')
                while signature_size is None or bytes_read < signature_size:
                    data = fread.read(signature_chunk_size)
                    if not data:
                        break
                    bytes_read += len(data)
                    signature.append(crc32(data))
            finally:
                if fread is not None:
                    fread.close()
            signatures[tuple(signature)].append(path)

    # If 'quick' is True then the signature comparison is adequate (although
    # it may result in false positives)
    if quick:
        for paths in signatures.itervalues():
            if len(paths) > 1:
                yield paths
        return

    def identical(p1, p2):
        """ Returns True if the contests of two files are identical. """
        f1, f2 = None, None
        try:
            f1 = fs.open(p1, 'rb')
            f2 = fs.open(p2, 'rb')
            while True:
                chunk1 = f1.read(16384)
                if not chunk1:
                    break
                chunk2 = f2.read(16384)
                if chunk1 != chunk2:
                    return False
            return True
        finally:
            if f1 is not None:
                f1.close()
            if f2 is not None:
                f2.close()

    # If we want to be accurate then we need to compare suspected duplicates
    # byte by byte.
    # All path groups in this loop have the same size and same signature, so are
    # highly likely to be identical.
    for paths in signatures.itervalues():

        while len(paths) > 1:

            test_p = paths.pop()
            dups = [test_p]

            for path in paths:
                if identical(test_p, path):
                    dups.append(path)

            if len(dups) > 1:
                yield dups

            paths = list(set(paths).difference(dups))


if __name__ == "__main__":
    from osfs import *
    fs = OSFS('~/copytest')
    
    from memoryfs import *
    m = MemoryFS()
    m.makedir('maps')
    
    copydir((fs, 'maps'), (m, 'maps'))
    
    from browsewin import browse
    browse(m)

