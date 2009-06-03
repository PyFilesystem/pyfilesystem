"""

  fs.helpers: useful standalone functions for FS path manipulation.

"""

from itertools import chain


def iteratepath(path, numsplits=None):
    """Iterate over the individual components of a path."""
    path = makerelative(normpath(path))
    if not path:
        return []
    if numsplits == None:
        return path.split('/')
    else:
        return path.split('/', numsplits)


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


def abspath(path):
    """Convert the given path to a normalized, absolute path.

    path -- A FS path
    """
    path = normpath(path)
    if not path or path[0] != "/":
        path = "/" + path
    return path


def relpath(path):
    """Convert the given path to a normalized, relative path.

    path -- A FS path
    """
    path = normpath(path)
    if path and path[0] == "/":
        path = path[1:]
    return path


def pathjoin(*paths):
    """Joins any number of paths together, returning a new path string.

    paths -- An iterable of path strings

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


def pathsplit(path):
    """Splits a path on a path separator. Returns a tuple containing the path up
    to that last separator and the remaining path component.

    path -- A FS path

    >>> pathsplit("foo/bar")
    ('foo', 'bar')

    >>> pathsplit("foo/bar/baz")
    ('foo/bar', 'baz')

    """
    split = normpath(path).rsplit('/', 1)
    if len(split) == 1:
        return ('', split[0])
    return tuple(split)


def dirname(path):
    """Returns the parent directory of a path.

    path -- A FS path

    >>> dirname('foo/bar/baz')
    'foo/bar'

    """
    return pathsplit(path)[0]


def resourcename(path):
    """Returns the resource references by a path.

    path -- A FS path

    >>> resourcename('foo/bar/baz')
    'baz'

    """
    return pathsplit(path)[1]


def makerelative(path):
    """Makes a path relative by removing initial separator.

    path -- A FS path

    >>> makerelative("/foo/bar")
    'foo/bar'

    """
    path = normpath(path)
    if path.startswith('/'):
        return path[1:]
    return path


def makeabsolute(path):
    """Makes a path absolute by adding a separator at the start of the path.

    path -- A FS path

    >>> makeabsolute("foo/bar/baz")
    '/foo/bar/baz'

    """
    path = normpath(path)
    if not path.startswith('/'):
        return '/'+path
    return path


def issamedir(path1, path2):
    """Return true if two paths reference a resource in the same directory.

    path1 -- First path
    path2 -- Second path

    >>> issamedir("foo/bar/baz.txt", "foo/bar/spam.txt")
    True
    >>> issamedir("foo/bar/baz/txt", "spam/eggs/spam.txt")
    False
    """
    return pathsplit(normpath(path1))[0] == pathsplit(normpath(path2))[0]


def isprefix(path1,path2):
    """Return true is path1 is a prefix of path2."""
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
        
    

