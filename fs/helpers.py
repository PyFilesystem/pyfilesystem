from itertools import chain

def isabsolutepath(path):
    """Returns True if a given path is absolute.

    >>> isabsolutepath("a/b/c")
    False

    >>> isabsolutepath("/foo/bar")
    True

    """
    if path:
        return path[0] in '\\/'
    return False

def normpath(path):
    """Normalizes a path to be in the formated expected by FS objects.
    Returns a new path string.

    >>> normpath(r"foo\\bar\\baz")
    'foo/bar/baz'

    """
    return path.replace('\\', '/')


def pathjoin(*paths):
    """Joins any number of paths together. Returns a new path string.

    paths -- An iterable of path strings

    >>> pathjoin('foo', 'bar', 'baz')
    'foo/bar/baz'

    >>> pathjoin('foo/bar', '../baz')
    'foo/baz'

    """
    absolute = False

    relpaths = []
    for p in paths:
        if p:
         if p[0] in '\\/':
             del relpaths[:]
             absolute = True
         relpaths.append(p)

    pathstack = []

    for component in chain(*(normpath(path).split('/') for path in relpaths)):
        if component == "..":
            if not pathstack:
                raise ValueError("Relative path is invalid")
            sub = pathstack.pop()
        elif component == ".":
            pass
        elif component:
            pathstack.append(component)

    if absolute:
        return "/" + "/".join(pathstack)
    else:
        return "/".join(pathstack)


def pathsplit(path):
    """Splits a path on a path separator. Returns a tuple containing the path up
    to that last separator and the remaining path component.

    >>> pathsplit("foo/bar")
    ('foo', 'bar')

    >>> pathsplit("foo/bar/baz")
    ('foo/bar', 'baz')

    """

    split = normpath(path).rsplit('/', 1)
    if len(split) == 1:
        return ('', split[0])
    return tuple(split)

def getroot(path):
    return pathsplit(path)[0]

def getresourcename(path):
    return pathsplit(path)[1]

def resolvepath(path):
    """Normalises the path and removes any relative path components.

    path -- A path string

    >>> resolvepath(r"foo\\bar\\..\\baz")
    'foo/baz'

    """
    return pathjoin(path)

def makerelative(path):
    """Makes a path relative by removing initial separator.

    path -- A path

    >>> makerelative("/foo/bar")
    'foo/bar'

    """
    path = normpath(path)
    if path.startswith('/'):
        return path[1:]
    return path

def makeabsolute(path):
    """Makes a path absolute by adding a separater at the beginning of the path.

    path -- A path

    >>> makeabsolute("foo/bar/baz")
    '/foo/bar/baz'

    """
    path = normpath(path)
    if not path.startswith('/'):
        return '/'+path
    return path


def issamedir(path1, path2):
    dirname1, name1 = pathsplit(path1)
    dirname2, name2 = pathsplit(path2)
    return dirname1 == dirname2
