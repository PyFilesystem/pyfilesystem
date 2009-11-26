"""

  fs.path: useful functions for FS path manipulation.

This is broadly similar to the standard 'os.path' module but works with
paths in the canonical format expected by all FS objects (backslash-separated,
optional leading slash).

"""


def normpath(path):
    """Normalizes a path to be in the format expected by FS objects.

    This function remove any leading or trailing slashes, collapses
    duplicate slashes, replaces forward with backward slashes, and generally
    tries very hard to return a new path string the canonical FS format.
    If the path is invalid, ValueError will be raised.

    >>> normpath(r"foo\\bar\\baz")
    'foo/bar/baz'

    >>> normpath("/foo//bar/frob/../baz")
    '/foo/bar/baz'

    >>> normpath("foo/../../bar")
    Traceback (most recent call last)
        ...
    ValueError: too many backrefs in path 'foo/../../bar'

    """
    if not path:
        return path
    components = []
    for comp in path.replace('\\','/').split("/"):
        if not comp or comp == ".":
            pass
        elif comp == "..":
            try:
                components.pop()
            except IndexError:
                err = "too many backrefs in path '%s'" % (path,)
                raise ValueError(err)
        else:
            components.append(comp)
    if path[0] in "\\/":
        if not components:
            components = [""]
        components.insert(0,"")
    return "/".join(components)


def iteratepath(path, numsplits=None):
    """Iterate over the individual components of a path."""
    path = relpath(normpath(path))
    if not path:
        return []
    if numsplits == None:
        return path.split('/')
    else:
        return path.split('/', numsplits)


def abspath(path):
    """Convert the given path to an absolute path.

    Since FS objects have no concept of a 'current directory' this simply
    adds a leading '/' character if the path doesn't already have one.

    """
    if not path:
        return "/"
    if path[0] != "/":
        return "/" + path
    return path


def relpath(path):
    """Convert the given path to a relative path.

    This is the inverse of abspath(), stripping a leading '/' from the
    path if it is present.

    """
    while path and path[0] == "/":
        path = path[1:]
    return path


def pathjoin(*paths):
    """Joins any number of paths together, returning a new path string.

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
             if p[0] in '\\/':
                 del relpaths[:]
                 absolute = True
             relpaths.append(p)

    path = normpath("/".join(relpaths))
    if absolute and not path.startswith("/"):
        path = "/" + path
    return path

# Allow pathjoin() to be used as fs.path.join()
join = pathjoin


def pathsplit(path):
    """Splits a path into (head,tail) pair.

    This function splits a path into a pair (head,tail) where 'tail' is the
    last pathname component and 'head' is all preceeding components.

    >>> pathsplit("foo/bar")
    ('foo', 'bar')

    >>> pathsplit("foo/bar/baz")
    ('foo/bar', 'baz')

    """
    split = normpath(path).rsplit('/', 1)
    if len(split) == 1:
        return ('', split[0])
    return tuple(split)

# Allow pathsplit() to be used as fs.path.split()
split = pathsplit


def dirname(path):
    """Returns the parent directory of a path.

    This is always equivalent to the 'head' component of the value returned
    by pathsplit(path).

    >>> dirname('foo/bar/baz')
    'foo/bar'

    """
    return pathsplit(path)[0]


def basename(path):
    """Returns the basename of the resource referenced by a path.

    This is always equivalent to the 'head' component of the value returned
    by pathsplit(path).

    >>> basename('foo/bar/baz')
    'baz'

    """
    return pathsplit(path)[1]


def issamedir(path1, path2):
    """Return true if two paths reference a resource in the same directory.

    >>> issamedir("foo/bar/baz.txt", "foo/bar/spam.txt")
    True
    >>> issamedir("foo/bar/baz/txt", "spam/eggs/spam.txt")
    False

    """
    return pathsplit(normpath(path1))[0] == pathsplit(normpath(path2))[0]


def isprefix(path1,path2):
    """Return true is path1 is a prefix of path2.

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
    for (bit1,bit2) in zip(bits1,bits2):
        if bit1 != bit2:
            return False
    return True

def forcedir(path):
    if not path.endswith('/'):
        return path + '/'
    return path
