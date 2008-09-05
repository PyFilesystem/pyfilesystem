#!/usr/bin/env python

from fs import *

from zipfile import ZipFile

class ZipFS(FS):

    def __init__(self, zip_file, mode="r", compression="deflated", allowZip64=False):

        if compression == "deflated":
            compression_type = zipfile.ZIP_DEFLATED
        elif compression == "stored":
            compression_type = zipfile.ZIP_STORED
        else:
            raise ValueError("Compression should be 'deflated' (default) or 'stored'")

        self.zf = ZipFile(zip_file, mode, compression_type, )
