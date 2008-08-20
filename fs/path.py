#!/usr/bin/env python

from os import path

class Path(unicode):

    def join(self, *paths):
        return Path(path.join(self, *paths))

    def _get_ext(self):
        return path.splitext(self)[-1]
    ext = property(_get_ext, None, "Retrieve the extension")

    def _get_head(self):
        head, tail = path.split(self)
        return Path(head)
    head = property(_get_head, None, "Retrieve the head of the path")

    def _get_tail(self):
        head, tail = path.split(self)
        return Path(tail)
    tail = property(_get_tail, None, "Retrieve the head of the path")
    

    def __div__(self, pth):
        return self.join(pth)

if __name__ == "__main__":

    p1 = Path("a/b")
    p2 = p1.join("c/d.txt")
    print p1 / "d/e/f"
    print p2
    print p2.ext
    print p2.head
    print p2.tail
