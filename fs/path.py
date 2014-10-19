"""
fs.path
=======

Useful functions for FS path manipulation.

This is broadly similar to the standard ``os.path`` module but works with
paths in the canonical format expected by all FS objects (that is, separated
by forward slashes and with an optional leading slash).

"""

import re
import os


_requires_normalization = re.compile(r'(^|/)\.\.?($|/)|//').search


def normpath(path):
    """Normalizes a path to be in the format expected by FS objects.

    This function removes trailing slashes, collapses duplicate slashes,
    and generally tries very hard to return a new path in the canonical FS format.
    If the path is invalid, ValueError will be raised.

    :param path: path to normalize
    :returns: a valid FS path

    >>> normpath("/foo//bar/frob/../baz")
    '/foo/bar/baz'

    >>> normpath("foo/../../bar")
    Traceback (most recent call last)
        ...
    BackReferenceError: Too many backrefs in 'foo/../../bar'

    """

    if path in ('', '/'):
        return path

    # An early out if there is no need to normalize this path
    if not _requires_normalization(path):
        return path.rstrip('/')

    prefix = u'/' if path.startswith('/') else u''
    components = []
    append = components.append
    special = ('..', '.', '').__contains__
    try:
        for component in path.split('/'):
            if special(component):
                if component == '..':
                    components.pop()
            else:
                append(component)
    except IndexError:
        # Imported here because errors imports this module (path),
        # causing a circular import.
        from fs.errors import BackReferenceError
        raise BackReferenceError('Too many backrefs in \'%s\'' % path)
    return prefix + u'/'.join(components)


if os.sep != '/':
    def ospath(path):
        """Replace path separators in an OS path if required"""
        return path.replace(os.sep, '/')
else:
    def ospath(path):
        """Replace path separators in an OS path if required"""
        return path


def iteratepath(path, numsplits=None):
    """Iterate over the individual components of a path.

    :param path: Path to iterate over
    :numsplits: Maximum number of splits

    """
    path = relpath(normpath(path))
    if not path:
        return []
    if numsplits == None:
        return path.split('/')
    else:
        return path.split('/', numsplits)


def recursepath(path, reverse=False):
    """Returns intermediate paths from the root to the given path

    :param reverse: reverses the order of the paths

    >>> recursepath('a/b/c')
    ['/', u'/a', u'/a/b', u'/a/b/c']

    """

    if path in ('', '/'):
        return [u'/']

    path = abspath(normpath(path)) + '/'

    paths = [u'/']
    find = path.find
    append = paths.append
    pos = 1
    len_path = len(path)

    while pos < len_path:
        pos = find('/', pos)
        append(path[:pos])
        pos += 1

    if reverse:
        return paths[::-1]
    return paths


def isabs(path):
    """Return True if path is an absolute path."""
    return path.startswith('/')


def abspath(path):
    """Convert the given path to an absolute path.

    Since FS objects have no concept of a 'current directory' this simply
    adds a leading '/' character if the path doesn't already have one.

    """
    if not path.startswith('/'):
        return u'/' + path
    return path


def relpath(path):
    """Convert the given path to a relative path.

    This is the inverse of abspath(), stripping a leading '/' from the
    path if it is present.

    :param path: Path to adjust

    >>> relpath('/a/b')
    'a/b'

    """
    return path.lstrip('/')


def pathjoin(*paths):
    """Joins any number of paths together, returning a new path string.

    :param paths: Paths to join are given in positional arguments

    >>> pathjoin('foo', 'bar', 'baz')
    'foo/bar/baz'

    >>> pathjoin('foo/bar', '../baz')
    'foo/baz'

    >>> pathjoin('foo/bar', '/baz')
    '/baz'

    """
    absolute = False
    relpaths = []
    for p in paths:
        if p:
            if p[0] == '/':
                del relpaths[:]
                absolute = True
            relpaths.append(p)

    path = normpath(u"/".join(relpaths))
    if absolute:
        path = abspath(path)
    return path


def pathcombine(path1, path2):
    """Joins two paths together.

    This is faster than `pathjoin`, but only works when the second path is relative,
    and there are no backreferences in either path.

    >>> pathcombine("foo/bar", "baz")
    'foo/bar/baz'

    """
    if not path1:
        return path2.lstrip()
    return "%s/%s" % (path1.rstrip('/'), path2.lstrip('/'))


def join(*paths):
    """Joins any number of paths together, returning a new path string.

    This is a simple alias for the ``pathjoin`` function, allowing it to be
    used as ``fs.path.join`` in direct correspondence with ``os.path.join``.

    :param paths: Paths to join are given in positional arguments
    """
    return pathjoin(*paths)


def pathsplit(path):
    """Splits a path into (head, tail) pair.

    This function splits a path into a pair (head, tail) where 'tail' is the
    last pathname component and 'head' is all preceding components.

    :param path: Path to split

    >>> pathsplit("foo/bar")
    ('foo', 'bar')

    >>> pathsplit("foo/bar/baz")
    ('foo/bar', 'baz')

    >>> pathsplit("/foo/bar/baz")
    ('/foo/bar', 'baz')

    """
    if '/' not in path:
        return ('', path)
    split = path.rsplit('/', 1)
    return (split[0] or '/', split[1])


def split(path):
    """Splits a path into (head, tail) pair.

    This is a simple alias for the ``pathsplit`` function, allowing it to be
    used as ``fs.path.split`` in direct correspondence with ``os.path.split``.

    :param path: Path to split
    """
    return pathsplit(path)


def splitext(path):
    """Splits the extension from the path, and returns the path (up to the last
    '.' and the extension).

    :param path: A path to split

    >>> splitext('baz.txt')
    ('baz', 'txt')

    >>> splitext('foo/bar/baz.txt')
    ('foo/bar/baz', 'txt')

    """

    parent_path, pathname = pathsplit(path)
    if '.' not in pathname:
        return path, ''
    pathname, ext = pathname.rsplit('.', 1)
    path = pathjoin(parent_path, pathname)
    return path, '.' + ext


def isdotfile(path):
    """Detects if a path references a dot file, i.e. a resource who's name
    starts with a '.'

    :param path: Path to check

    >>> isdotfile('.baz')
    True

    >>> isdotfile('foo/bar/.baz')
    True

    >>> isdotfile('foo/bar.baz')
    False

    """
    return basename(path).startswith('.')


def dirname(path):
    """Returns the parent directory of a path.

    This is always equivalent to the 'head' component of the value returned
    by pathsplit(path).

    :param path: A FS path

    >>> dirname('foo/bar/baz')
    'foo/bar'

    >>> dirname('/foo/bar')
    '/foo'

    >>> dirname('/foo')
    '/'

    """
    return pathsplit(path)[0]


def basename(path):
    """Returns the basename of the resource referenced by a path.

    This is always equivalent to the 'tail' component of the value returned
    by pathsplit(path).

    :param path: A FS path

    >>> basename('foo/bar/baz')
    'baz'

    >>> basename('foo/bar')
    'bar'

    >>> basename('foo/bar/')
    ''

    """
    return pathsplit(path)[1]


def issamedir(path1, path2):
    """Return true if two paths reference a resource in the same directory.

    :param path1: An FS path
    :param path2: An FS path

    >>> issamedir("foo/bar/baz.txt", "foo/bar/spam.txt")
    True
    >>> issamedir("foo/bar/baz/txt", "spam/eggs/spam.txt")
    False

    """
    return dirname(normpath(path1)) == dirname(normpath(path2))


def isbase(path1, path2):
    p1 = forcedir(abspath(path1))
    p2 = forcedir(abspath(path2))
    return p1 == p2 or p1.startswith(p2)


def isprefix(path1, path2):
    """Return true is path1 is a prefix of path2.

    :param path1: An FS path
    :param path2: An FS path

    >>> isprefix("foo/bar", "foo/bar/spam.txt")
    True
    >>> isprefix("foo/bar/", "foo/bar")
    True
    >>> isprefix("foo/barry", "foo/baz/bar")
    False
    >>> isprefix("foo/bar/baz/", "foo/baz/bar")
    False

    """
    bits1 = path1.split("/")
    bits2 = path2.split("/")
    while bits1 and bits1[-1] == "":
        bits1.pop()
    if len(bits1) > len(bits2):
        return False
    for (bit1, bit2) in zip(bits1, bits2):
        if bit1 != bit2:
            return False
    return True


def forcedir(path):
    """Ensure the path ends with a trailing forward slash

    :param path: An FS path

    >>> forcedir("foo/bar")
    'foo/bar/'
    >>> forcedir("foo/bar/")
    'foo/bar/'

    """

    if not path.endswith('/'):
        return path + '/'
    return path


def frombase(path1, path2):
    if not isprefix(path1, path2):
        raise ValueError("path1 must be a prefix of path2")
    return path2[len(path1):]


def relativefrom(base, path):
    """Return a path relative from a given base path,
    i.e. insert backrefs as appropriate to reach the path from the base.

    :param base_path: Path to a directory
    :param path: Path you wish to make relative


    >>> relativefrom("foo/bar", "baz/index.html")
    '../../baz/index.html'

    """
    base = list(iteratepath(base))
    path = list(iteratepath(path))

    common = 0
    for a, b in zip(base, path):
        if a != b:
            break
        common += 1

    return u'/'.join([u'..'] * (len(base) - common) + path[common:])


class PathMap(object):
    """Dict-like object with paths for keys.

    A PathMap is like a dictionary where the keys are all FS paths.  It has
    two main advantages over a standard dictionary.  First, keys are normalized
    automatically::

        >>> pm = PathMap()
        >>> pm["hello/world"] = 42
        >>> print pm["/hello/there/../world"]
        42

    Second, various dictionary operations (e.g. listing or clearing values)
    can be efficiently performed on a subset of keys sharing some common
    prefix::

        # list all values in the map
        pm.values()

        # list all values for paths starting with "/foo/bar"
        pm.values("/foo/bar")

    Under the hood, a PathMap is a trie-like structure where each level is
    indexed by path name component.  This allows lookups to be performed in
    O(number of path components) while permitting efficient prefix-based
    operations.
    """

    def __init__(self):
        self._map = {}

    def __getitem__(self, path):
        """Get the value stored under the given path."""
        m = self._map
        for name in iteratepath(path):
            try:
                m = m[name]
            except KeyError:
                raise KeyError(path)
        try:
            return m[""]
        except KeyError:
            raise KeyError(path)

    def __contains__(self, path):
        """Check whether the given path has a value stored in the map."""
        try:
            self[path]
        except KeyError:
            return False
        else:
            return True

    def __setitem__(self, path, value):
        """Set the value stored under the given path."""
        m = self._map
        for name in iteratepath(path):
            try:
                m = m[name]
            except KeyError:
                m = m.setdefault(name, {})
        m[""] = value

    def __delitem__(self, path):
        """Delete the value stored under the given path."""
        ms = [[self._map, None]]
        for name in iteratepath(path):
            try:
                ms.append([ms[-1][0][name], None])
            except KeyError:
                raise KeyError(path)
            else:
                ms[-2][1] = name
        try:
            del ms[-1][0][""]
        except KeyError:
            raise KeyError(path)
        else:
            while len(ms) > 1 and not ms[-1][0]:
                del ms[-1]
                del ms[-1][0][ms[-1][1]]

    def get(self, path, default=None):
        """Get the value stored under the given path, or the given default."""
        try:
            return self[path]
        except KeyError:
            return default

    def pop(self, path, default=None):
        """Pop the value stored under the given path, or the given default."""
        ms = [[self._map, None]]
        for name in iteratepath(path):
            try:
                ms.append([ms[-1][0][name], None])
            except KeyError:
                return default
            else:
                ms[-2][1] = name
        try:
            val = ms[-1][0].pop("")
        except KeyError:
            val = default
        else:
            while len(ms) > 1 and not ms[-1][0]:
                del ms[-1]
                del ms[-1][0][ms[-1][1]]
        return val

    def setdefault(self, path, value):
        m = self._map
        for name in iteratepath(path):
            try:
                m = m[name]
            except KeyError:
                m = m.setdefault(name, {})
        return m.setdefault("", value)

    def clear(self, root="/"):
        """Clear all entries beginning with the given root path."""
        m = self._map
        for name in iteratepath(root):
            try:
                m = m[name]
            except KeyError:
                return
        m.clear()

    def iterkeys(self, root="/", m=None):
        """Iterate over all keys beginning with the given root path."""
        if m is None:
            m = self._map
            for name in iteratepath(root):
                try:
                    m = m[name]
                except KeyError:
                    return
        for (nm, subm) in m.iteritems():
            if not nm:
                yield abspath(root)
            else:
                k = pathcombine(root, nm)
                for subk in self.iterkeys(k, subm):
                    yield subk

    def __iter__(self):
        return self.iterkeys()

    def keys(self,root="/"):
        return list(self.iterkeys(root))

    def itervalues(self, root="/", m=None):
        """Iterate over all values whose keys begin with the given root path."""
        root = normpath(root)
        if m is None:
            m = self._map
            for name in iteratepath(root):
                try:
                    m = m[name]
                except KeyError:
                    return
        for (nm, subm) in m.iteritems():
            if not nm:
                yield subm
            else:
                k = pathcombine(root, nm)
                for subv in self.itervalues(k, subm):
                    yield subv

    def values(self, root="/"):
        return list(self.itervalues(root))

    def iteritems(self, root="/", m=None):
        """Iterate over all (key,value) pairs beginning with the given root."""
        root = normpath(root)
        if m is None:
            m = self._map
            for name in iteratepath(root):
                try:
                    m = m[name]
                except KeyError:
                    return
        for (nm, subm) in m.iteritems():
            if not nm:
                yield (abspath(normpath(root)), subm)
            else:
                k = pathcombine(root, nm)
                for (subk, subv) in self.iteritems(k, subm):
                    yield (subk, subv)

    def items(self, root="/"):
        return list(self.iteritems(root))

    def iternames(self, root="/"):
        """Iterate over all names beneath the given root path.

        This is basically the equivalent of listdir() for a PathMap - it yields
        the next level of name components beneath the given path.
        """
        m = self._map
        for name in iteratepath(root):
            try:
                m = m[name]
            except KeyError:
                return
        for (nm, subm) in m.iteritems():
            if nm and subm:
                yield nm

    def names(self, root="/"):
        return list(self.iternames(root))


_wild_chars = frozenset('*?[]!{}')


def iswildcard(path):
    """Check if a path ends with a wildcard

    >>> is_wildcard('foo/bar/baz.*')
    True
    >>> is_wildcard('foo/bar')
    False

    """
    assert path is not None
    return not _wild_chars.isdisjoint(path)

if __name__ == "__main__":
    print recursepath('a/b/c')

    print relativefrom('/', '/foo')
    print relativefrom('/foo/bar', '/foo/baz')
    print relativefrom('/foo/bar/baz', '/foo/egg')
    print relativefrom('/foo/bar/baz/egg', '/foo/egg')
