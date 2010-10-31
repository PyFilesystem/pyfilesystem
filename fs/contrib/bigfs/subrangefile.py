"""

fs.contrib.bigfs.subrangefile
=============================

A file-like object that allows wrapping of part of a binary file for reading.

This avoids needless copies of data for large binary files if StringIO would
be used.

Written by Koen van de Sande
http://www.tibed.net

Contributed under the terms of the BSD License:
http://www.opensource.org/licenses/bsd-license.php
"""


class SubrangeFile:
    """File-like class with read-only, binary mode restricting access to a subrange of the whole file"""
    def __init__(self, f, startOffset, fileSize):
        if not hasattr(f, 'read'):
            self.f = open(f, "rb")
            self.name = f
        else:
            self.f = f
            self.name = str(f)
        self.startOffset = startOffset
        self.fileSize = fileSize
        self.seek(0)
        
    def __str__(self):
        return "<SubrangeFile: %s@%d size=%d>" % (self.name, self.startOffset, self.fileSize)

    def __unicode__(self):
        return unicode(self.__str__())

    def size(self):
        return self.fileSize

    def seek(self, offset, whence=0):
        if whence == 0:
            offset = self.startOffset + offset
        elif whence == 1:
            offset = self.startOffset + self.tell() + offset
        elif whence == 2:
            if offset > 0:
                offset = 0
            offset = self.startOffset + self.fileSize + offset
        self.f.seek(offset)
        
    def tell(self):
        return self.f.tell() - self.startOffset

    def __maxSize(self,size=None):
        iSize = self.fileSize
        if not size is None:
            if size < iSize:
                iSize = size
        if self.tell() + iSize > self.fileSize:
            iSize = self.fileSize - self.tell()
        return iSize
            
    def readline(self,size=None):
        toRead = self.__maxSize(size)
        return self.f.readline(toRead)

    def read(self,size=None):
        toRead = self.__maxSize(size)
        return self.f.read(toRead)

    def readlines(self,size=None):
        toRead = self.__maxSize(size)
        temp = self.f.readlines(toRead)
        # now cut off more than we should read...
        result = []
        counter = 0
        for line in temp:
            if counter + len(line) > toRead:
                if toRead == counter:
                    break
                result.append(line[0:(toRead-counter)])
                break
            else:
                result.append(line)
                counter += len(line)
        return result


