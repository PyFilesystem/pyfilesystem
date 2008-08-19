import shutil

def copyfile(src_fs, src_path, dst_fs, dst_path, chunk_size=1024*16):

    def getsyspath(_fs, path):
        try:
            return _fs.getsyspath(path)
        except NoSysPathError:
            return ""

    src_syspath = src_fs.getsyspath(src_path)
    dst_syspath = dst_fs.getsyspath(dst_path)

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