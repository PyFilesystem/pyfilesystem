"""
fs.contrib.bigfs
================

A FS object that represents the contents of a BIG file 
(C&C Generals, BfME C&C3, C&C Red Alert 3, C&C4 file format)

Written by Koen van de Sande
http://www.tibed.net

Contributed under the terms of the BSD License:
http://www.opensource.org/licenses/bsd-license.php
"""

from struct import pack, unpack

from fs.base import *
from fs.memoryfs import MemoryFS
from fs.filelike import StringIO

from fs.contrib.bigfs.subrangefile import SubrangeFile

class BIGEntry:
    def __init__(self, filename, offset, storedSize, isCompressed, realSize):
        self.filename = filename
        self.offset = offset
        self.storedSize = storedSize
        self.realSize = realSize
        self.isCompressed = isCompressed

    def getfile(self, baseFile):
        f = SubrangeFile(baseFile, self.offset, self.storedSize)
        if not self.isCompressed:
            return f
        else:
            return self.decompress(f, wrapAsFile=True)
    
    def getcontents(self, baseFile):
        f = SubrangeFile(baseFile, self.offset, self.storedSize)
        if not self.isCompressed:
            return f.read()
        else:
            return self.decompress(f, wrapAsFile=False)
    
    def decompress(self, g, wrapAsFile=True):
        buf = g.read(2)
        magic = unpack(">H", buf)[0]
        if (magic & 0x3EFF) == 0x10FB:
            # it is compressed
            if magic & 0x8000:
                outputSize = unpack(">I", g.read(4))[0]
                if magic & 0x100:
                    unknown1 = unpack(">I", g.read(4))[0]
            else:
                outputSize = unpack(">I", "\0" + g.read(3))[0]
                if magic & 0x100:
                    unknown1 = unpack(">I", "\0" + g.read(3))[0]
        
        output = []
        while True:
            opcode = unpack("B", g.read(1))[0]
            if not (opcode & 0x80):       # opcode: bit7==0 to get here
                # read second opcode
                opcode2 = unpack("B", g.read(1))[0]
                #print "0x80", toBits(opcode), toBits(opcode2), opcode & 0x03, (((opcode & 0x60) << 3) | opcode2) + Q, ((opcode & 0x1C) >> 2) + 2 + R
    
                # copy at most 3 bytes to output stream (lowest 2 bits of opcode)
                count = opcode & 0x03
                for i in range(count):
                    output.append(g.read(1))
                
                # you always have to look at least one byte, hence the +1
                # use bit6 and bit5 (bit7=0 to trigger the if-statement) of opcode, and 8 bits of opcode2 (10-bits)
                lookback = (((opcode & 0x60) << 3) | opcode2) + 1
                
                # use bit4..2 of opcode
                count = ((opcode & 0x1C) >> 2) + 3
                
                for i in range(count):
                    output.append(output[-lookback])
            elif not (opcode & 0x40):     # opcode: bit7..6==10 to get here
                opcode2 = unpack("B", g.read(1))[0]
                opcode3 = unpack("B", g.read(1))[0]
                #print "0x40", toBits(opcode), toBits(opcode2), toBits(opcode3)
                
                # copy count bytes (upper 2 bits of opcode2)
                count = opcode2 >> 6
                for i in range(count):
                    output.append(g.read(1))
                
                # look back again (lower 6 bits of opcode2, all 8 bits of opcode3, total 14-bits)
                lookback = (((opcode2 & 0x3F) << 8) | opcode3) + 1
                # lower 6 bits of opcode are the count to copy
                count = (opcode & 0x3F) + 4
                
                for i in range(count):
                    output.append(output[-lookback])
            elif not (opcode & 0x20):     # opcode: bit7..5=110 to get here
                opcode2 = unpack("B", g.read(1))[0]
                opcode3 = unpack("B", g.read(1))[0]
                opcode4 = unpack("B", g.read(1))[0]

                # copy at most 3 bytes to output stream (lowest 2 bits of opcode)
                count = opcode & 0x03
                for i in range(count):
                    output.append(g.read(1))
                
                # look back: bit4 of opcode, all bits of opcode2 and opcode3, total 17-bits
                lookback = (((opcode & 0x10) >> 4) << 16) | (opcode2 << 8) | (opcode3) + 1
                # bit3..2 of opcode and the whole of opcode4
                count = (((((opcode & 0x0C) >> 2) << 8)) | opcode4) + 5

                #print "0x20", toBits(opcode), toBits(opcode2), toBits(opcode3), toBits(opcode4), lookback, count

                for i in range(count):
                    output.append(output[-lookback])
            else:                         # opcode: bit7..5==1 to get here
                # use lowest 5 bits for count
                count = ((opcode & 0x1F) << 2) + 4
                if count > 0x70:   # this is end of input
                    # turn into a small-copy
                    count = opcode & 0x03
                    #print "0xEXITCOPY", count
                    for i in range(count):
                        output.append(g.read(1))
                    break

                # "big copy" operation: up to 112 bytes (minumum of 4, multiple of 4)
                for i in range(count):
                    output.append(g.read(1))
                #print "0xLO", toBits(opcode), count
        
        if wrapAsFile:
            return StringIO("".join(output))
        else:
            return "".join(output)
        
    def __str__(self):
        return "<BIGEntry %s offset=%d storedSize=%d isCompressed=%s realSize=%d in %s" % (self.filename, self.offset, self.storedSize, str(self.isCompressed), self.realSize, self.filenameBIG)
        

class _ExceptionProxy(object):

    """A placeholder for an object that may no longer be used."""

    def __getattr__(self, name):
        raise ValueError("File has been closed")

    def __setattr__(self, name, value):
        raise ValueError("File has been closed")

    def __nonzero__(self):
        return False


class BigFS(FS):

    """A FileSystem that represents a BIG file."""
    
    _meta = { 'virtual' : False,
              'read_only' : True,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'network' : False,                        
             }

    def __init__(self, filename, mode="r", thread_synchronize=True):
        """Create a FS that maps on to a big file.

        :param filename: A (system) path, or a file-like object
        :param mode: Mode to open file: 'r' for reading, 'w' and 'a' not supported
        :param thread_synchronize: -- Set to True (default) to enable thread-safety

        """
        super(BigFS, self).__init__(thread_synchronize=thread_synchronize)

        if len(mode) > 1 or mode not in "r":
            raise ValueError("mode must be 'r'")
        self.file_mode = mode
        self.big_path = str(filename)

        self.entries = {}
        try:
            self.bf = open(filename, "rb")
        except IOError:
            raise ResourceNotFoundError(str(filename), msg="BIG file does not exist: %(path)s")

        self._path_fs = MemoryFS()
        if mode in 'ra':
            self._parse_resource_list(self.bf)

    def __str__(self):
        return "<BigFS: %s>" % self.big_path

    def __unicode__(self):
        return unicode(self.__str__())


    def _parse_resource_list(self, g):
        magicWord = g.read(4)
        if magicWord != "BIGF" and magicWord != "BIG4":
            raise ValueError("Magic word of BIG file invalid: " + filename + " " + repr(magicWord))
        header = g.read(12)
        header = unpack(">III", header)
        BIGSize = header[0]
        fileCount = header[1]
        bodyOffset = header[2]
        for i in range(fileCount):
            fileHeader = g.read(8)
            fileHeader = unpack(">II", fileHeader)

            pos = g.tell()
            buf = g.read(4096)
            marker = buf.find("\0")
            if marker == -1:
                raise ValueError("Could not parse filename in BIG file: Too long or invalid file")
            name = buf[:marker]
            # TODO: decode the encoding of name (or normalize the path?)
            isCompressed, uncompressedSize = self.__isCompressed(g, fileHeader[0], fileHeader[1])
            be = BIGEntry(name, fileHeader[0], fileHeader[1], isCompressed, uncompressedSize)
            name = normpath(name)
            self.entries[name] = be
            self._add_resource(name)
            g.seek(pos + marker + 1)

    def __isCompressed(self, g, offset, size):
        g.seek(offset)
        buf = g.read(2)
        magic = unpack(">H", buf)[0]
        if (magic & 0x3EFF) == 0x10FB:
            # it is compressed
            if magic & 0x8000:
                # decompressed size is uint32
                return True, unpack(">I", g.read(4))[0]
            else:
                # use only 3 bytes
                return True, unpack(">I", "\0" + g.read(3))[0]
        return False, size

    def _add_resource(self, path):
        if path.endswith('/'):
            path = path[:-1]
            if path:
                self._path_fs.makedir(path, recursive=True, allow_recreate=True)
        else:
            dirpath, filename = pathsplit(path)
            if dirpath:
                self._path_fs.makedir(dirpath, recursive=True, allow_recreate=True)
            f = self._path_fs.open(path, 'w')
            f.close()


    def close(self):
        """Finalizes the zip file so that it can be read.
        No further operations will work after this method is called."""

        if hasattr(self, 'bf') and self.bf:
            self.bf.close()
            self.bf = _ExceptionProxy()

    @synchronize
    def open(self, path, mode="r", **kwargs):
        path = normpath(relpath(path))        

        if 'r' in mode:
            if self.file_mode not in 'ra':
                raise OperationFailedError("open file", path=path, msg="Big file must be opened for reading ('r') or appending ('a')")
            try:
                return self.entries[path].getfile(self.bf)
            except KeyError:
                raise ResourceNotFoundError(path)

        if 'w' in mode:
            raise OperationFailedError("open file", path=path, msg="Big file cannot be edited ATM")

        raise ValueError("Mode must contain be 'r' or 'w'")

    @synchronize
    def getcontents(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        path = normpath(path)
        try:
            contents = self.entries[path].getcontents(self.bf)
        except KeyError:
            raise ResourceNotFoundError(path)
        except RuntimeError:
            raise OperationFailedError("read file", path=path, msg="Big file must be oppened with 'r' or 'a' to read")
        return contents

    def desc(self, path):
        if self.isdir(path):
            return "Dir in big file: %s" % self.big_path
        else:
            return "File in big file: %s" % self.big_path

    def isdir(self, path):
        return self._path_fs.isdir(path)

    def isfile(self, path):
        return self._path_fs.isfile(path)

    def exists(self, path):
        return self._path_fs.exists(path)

    @synchronize
    def makedir(self, dirname, recursive=False, allow_recreate=False):
        dirname = normpath(dirname)
        if self.file_mode not in "wa":
            raise OperationFailedError("create directory", path=dirname, msg="Big file must be opened for writing ('w') or appending ('a')")
        if not dirname.endswith('/'):
            dirname += '/'
        self._add_resource(dirname)

    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        return self._path_fs.listdir(path, wildcard, full, absolute, dirs_only, files_only)

    @synchronize
    def getinfo(self, path):
        if not self.exists(path):
            raise ResourceNotFoundError(path)
        path = normpath(path).lstrip('/')
        info = {'size': 0}
        if path in self.entries:
            be = self.entries[path]
            info['size'] = be.realSize
            info['file_size'] = be.realSize
            info['stored_size'] = be.storedSize
            info['is_compressed'] = be.isCompressed
            info['offset'] = be.offset
            info['internal_filename'] = be.filename
            info['filename'] = path
        return info
