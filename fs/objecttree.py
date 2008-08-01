#!/usr/bin/env python

from fs import _iteratepath, pathsplit

class _ObjectDict(dict):
    pass

class ObjectTree(object):

    """A class to facilitate the creation of tree structures."""

    def __init__(self):
        self.root = _ObjectDict()

    def _locate(self, path):
        current = self.root
        for path_component in path.split('/'):
            if type(current) is not _ObjectDict:
                return None
            node = current.get(path_component, None)
            if node is None:
                return None
            current = node
        return node

    def __setitem__(self, path, object):
        if not path:
            raise IndexError("No path supplied")
        current = self.root
        path, name = path.rsplit('/', 1)
        for path_component in path.split('/'):
            node = current.get(path_component, None)
            if type(node) is not _ObjectDict:
                new_dict = _ObjectDict()
                current[path_component] = new_dict
                current = new_dict
            else:
                current = node
        current[name] = object

    def __getitem__(self, path):
        node = self._locate(path)
        if node is None:
            raise IndexError("Path does not exist")
        return node

    def get(self, path, default):
        node = self._locate(path)
        if node is None:
            return default
        return node

    def isobject(self, path):
        node = self._locate(path)
        return type(node) is not _ObjectDict

    def __contains__(self, path):
        node = self._locate(path)
        return node is not None

    def __iter__(self):
        return iter(self.root)

    def keys(self):
        return self.root.keys()

    def iterkeys(self):
        return self.root.keys()

    def items(self):
        return self.root.items()

    def iteritems(self):
        return self.root.iteritems()

if __name__ == "__main__":

    ot = ObjectTree()
    ot['a/b/c'] = "Hai!"

    print ot['a/b/c']

    ot['a/b/c/d'] = "?"

    print ot['a/b/c'].keys()