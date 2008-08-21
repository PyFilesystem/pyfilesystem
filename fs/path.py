#!/usr/bin/env python
import fs
from os import path

class Path(unicode):

    def __repr__(self):
        return "Path(%s)" % unicode.__repr__(self)

    def __add__(self, path):
        return self.__class__(unicode.__add__(self, path))

    def __radd__(self, path):
        return self.__class__(path.__add__(self))

    def join(self, *paths):
        return self.__class__(fs.pathjoin(self, *paths))

    def _get_ext(self):
        return path.splitext(self)[-1]
    ext = property(_get_ext, None, "Retrieve the extension")

    def _get_head(self):
        head, tail = path.split(self)
        return self.__class__(head)
    head = property(_get_head, None, "Retrieve the head of the path")

    def _get_tail(self):
        head, tail = path.split(self)
        return self.__class__(tail)
    tail = property(_get_tail, None, "Retrieve the head of the path")

    def splitall(self):
        return [p for p in self.split('/') if p]

    def replace(self, s1, s2):
        return self.__class__(unicode.replace(self, s1, s2))

    def __getitem__(self, slice):
        return self.__class__(unicode.__getitem__(self, slice))

    def __div__(self, pth):
        return self.join(pth)
    __truediv__ = __div__

if __name__ == "__main__":

    p1 = Path("a/b")
    p2 = p1.join("c/d.txt")
    print repr(p1.replace('a', 'HAI!'))
    print repr(p1[0])
    print repr(p1 + p2)
    print p1 / "d/e/f"
    print p2
    print p2.ext
    print p2.head
    print p2.tail
    print p2
    print p2.splitall()
