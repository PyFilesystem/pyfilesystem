import shutil

def copy_file(src_fs, src_path, dst_fs, dst_path, chunk_size=1024*16):

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

def get_total_data(count_fs):

    """Returns the total number of bytes contained within files.

    count_fs -- A filesystem object

    """

    total = 0
    for f in count_fs.walkfiles(absolute=True):
        total += count_fs.getsize(f)
    return total
