

class _ObjectDict(dict):
    pass


class ObjectTree(object):
    """A class to facilitate the creation of tree structures."""

    def __init__(self):
        self.root = _ObjectDict()

    def _split(self, path):
        if '/' not in path:
            return  "", path
        else:
            return path.rsplit('/', 1)

    def _splitpath(self, path):
        return [p for p in path.split('/') if p]

    def _locate(self, path):
        current = self.root
        for path_component in self._splitpath(path):
            if type(current) is not _ObjectDict:
                return None
            node = current.get(path_component, None)
            if node is None:
                return None
            current = node
        return current

    def __setitem__(self, path, object):
        if not path:
            raise IndexError("No path supplied")
        current = self.root
        path, name = self._split(path)
        for path_component in self._splitpath(path):
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

    def __delitem__(self, path):
        path, name = self._split(path)
        node = self._locate(path)
        if node is None or type(node) is not _ObjectDict:
            raise IndexError("Path does not exist")
        del node[name]

    def get(self, path, default):
        node = self._locate(path)
        if node is None:
            return default
        return node

    def partialget(self, path, default=None):
        current = self.root
        partial_path = []
        remaining_path = self._splitpath(path)
        for path_component in remaining_path[:]:
            if type(current) is not _ObjectDict:
                return "/".join(partial_path), current, "/".join(remaining_path)
            partial_path.append(path_component)
            remaining_path.pop(0)
            node = current.get(path_component, None)
            if node is None:
                return None, default, None
            current = node
        return path, current, ""

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
        return self.root.iterkeys()

    def items(self):
        return self.root.items()

    def iteritems(self):
        return self.root.iteritems()


