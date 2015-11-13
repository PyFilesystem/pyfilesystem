"""
fs.opener
=========

Open filesystems via a URI.

There are occasions when you want to specify a filesystem from the command line
or in a config file. This module enables that functionality, and can return an
FS object given a filesystem specification in a URI-like syntax (inspired by
the syntax of http://commons.apache.org/vfs/filesystems.html).

The `OpenerRegistry` class maps the protocol (file, ftp etc.) on to an Opener
object, which returns an appropriate filesystem object and path.  You can
create a custom opener registry that opens just the filesystems you require, or
use the opener registry defined here (also called `opener`) that can open any
supported filesystem.

The `parse` method of an `OpenerRegsitry` object returns a tuple of an FS
object a path. Here's an example of how to use the default opener registry::

    >>> from fs.opener import opener
    >>> opener.parse('ftp://ftp.mozilla.org/pub')
    (<fs.ftpfs.FTPFS object at 0x96e66ec>, u'pub')

You can use use the `opendir` method, which just returns an FS object. In the
example above, `opendir` will return a FS object for the directory `pub`::

    >>> opener.opendir('ftp://ftp.mozilla.org/pub')
    <SubFS: <FTPFS ftp.mozilla.org>/pub>

If you are just interested in a single file, use the `open` method of a registry
which returns a file-like object, and has the same signature as FS objects and
the `open` builtin::

    >>> opener.open('ftp://ftp.mozilla.org/pub/README')
    <fs.ftpfs._FTPFile object at 0x973764c>

The `opendir` and `open` methods can also be imported from the top-level of
this module for sake of convenience.  To avoid shadowing the builtin `open`
method, they are named `fsopendir` and `fsopen`. Here's how you might import
them::

    from fs.opener import fsopendir, fsopen


"""

__all__ = ['OpenerError',
           'NoOpenerError',
           'OpenerRegistry',
           'opener',
           'fsopen',
           'fsopendir',
           'OpenerRegistry',
           'Opener',
           'OSFSOpener',
           'ZipOpener',
           'RPCOpener',
           'FTPOpener',
           'SFTPOpener',
           'MemOpener',
           'DebugOpener',
           'TempOpener',
           'S3Opener',
           'TahoeOpener',
           'DavOpener',
           'HTTPOpener']

from fs.path import pathsplit, join, iswildcard, normpath
from fs.osfs import OSFS
from fs.filelike import FileWrapper
from os import getcwd
import os.path
import re
from urlparse import urlparse

class OpenerError(Exception):
    """The base exception thrown by openers"""
    pass

class NoOpenerError(OpenerError):
    """Thrown when there is no opener for the given protocol"""
    pass

def _expand_syspath(path):
    if path is None:
        return path
    if path.startswith('\\\\?\\'):
        path = path[4:]
    path = os.path.expanduser(os.path.expandvars(path))
    path = os.path.normpath(os.path.abspath(path))
    return path

def _parse_credentials(url):
    scheme = None
    if '://' in url:
        scheme, url = url.split('://', 1)
    username = None
    password = None
    if '@' in url:
        credentials, url = url.split('@', 1)
        if ':' in credentials:
            username, password = credentials.split(':', 1)
        else:
            username = credentials
    if scheme is not None:
        url = '%s://%s' % (scheme, url)
    return username, password, url

def _parse_name(fs_name):
    if '#' in fs_name:
        fs_name, fs_name_params = fs_name.split('#', 1)
        return fs_name, fs_name_params
    else:
        return fs_name, None

def _split_url_path(url):
    if '://' not in url:
        url = 'http://' + url
    scheme, netloc, path, _params, _query, _fragment = urlparse(url)
    url = '%s://%s' % (scheme, netloc)
    return url, path


def _FSClosingFile(fs, file_object, mode):
    original_close = file_object.close

    def close():
        try:
            fs.close()
        except:
            pass
        return original_close()
    file_object.close = close
    return file_object


class OpenerRegistry(object):

    """An opener registry that  stores a number of opener objects used to parse FS URIs"""

    re_fs_url = re.compile(r'''
^
(.*?)
:\/\/

(?:
(?:(.*?)@(.*?))
|(.*?)
)

(?:
!(.*?)$
)*$
''', re.VERBOSE)


    def __init__(self, openers=[]):
        self.registry = {}
        self.openers = {}
        self.default_opener = 'osfs'
        for opener in openers:
            self.add(opener)

    @classmethod
    def split_segments(self, fs_url):
        match = self.re_fs_url.match(fs_url)
        return match

    def get_opener(self, name):
        """Retrieve an opener for the given protocol

        :param name: name of the opener to open
        :raises NoOpenerError: if no opener has been registered of that name

        """
        if name not in self.registry:
            raise NoOpenerError("No opener for %s" % name)
        index = self.registry[name]
        return self.openers[index]

    def add(self, opener):
        """Adds an opener to the registry

        :param opener: a class derived from fs.opener.Opener

        """

        index = len(self.openers)
        self.openers[index] = opener
        for name in opener.names:
            self.registry[name] = index

    def parse(self, fs_url, default_fs_name=None, writeable=False, create_dir=False, cache_hint=True):
        """Parses a FS url and returns an fs object a path within that FS object
        (if indicated in the path). A tuple of (<FS instance>, <path>) is returned.

        :param fs_url: an FS url
        :param default_fs_name: the default FS to use if none is indicated (defaults is OSFS)
        :param writeable: if True, a writeable FS will be returned
        :param create_dir: if True, then the directory in the FS will be created

        """

        orig_url = fs_url
        match = self.split_segments(fs_url)

        if match:
            fs_name, credentials, url1, url2, path = match.groups()
            if credentials:
                fs_url = '%s@%s' % (credentials, url1)
            else:
                fs_url = url2
            path = path or ''
            fs_url = fs_url or ''
            if ':' in fs_name:
                fs_name, sub_protocol = fs_name.split(':', 1)
                fs_url = '%s://%s' % (sub_protocol, fs_url)
            if '!' in path:
                paths = path.split('!')
                path = paths.pop()
                fs_url = '%s!%s' % (fs_url, '!'.join(paths))

            fs_name = fs_name or self.default_opener
        else:
            fs_name = default_fs_name or self.default_opener
            fs_url = _expand_syspath(fs_url)
            path = ''

        fs_name,  fs_name_params = _parse_name(fs_name)
        opener = self.get_opener(fs_name)

        if fs_url is None:
            raise OpenerError("Unable to parse '%s'" % orig_url)

        fs, fs_path = opener.get_fs(self, fs_name, fs_name_params, fs_url, writeable, create_dir)
        fs.cache_hint(cache_hint)

        if fs_path and iswildcard(fs_path):
            pathname, resourcename = pathsplit(fs_path or '')
            if pathname:
                fs = fs.opendir(pathname)
            return fs, resourcename

        fs_path = join(fs_path, path)

        if create_dir and fs_path:
            if not fs.getmeta('read_only', False):
                fs.makedir(fs_path, allow_recreate=True)

        pathname, resourcename = pathsplit(fs_path or '')
        if pathname and resourcename:
            fs = fs.opendir(pathname)
            fs_path = resourcename

        return fs, fs_path or ''

    def open(self, fs_url, mode='r', **kwargs):
        """Opens a file from a given FS url

        If you intend to do a lot of file manipulation, it would likely be more
        efficient to do it directly through the an FS instance (from `parse` or
        `opendir`). This method is fine for one-offs though.

        :param fs_url: a FS URL, e.g. ftp://ftp.mozilla.org/README
        :param mode: mode to open file file
        :rtype: a file

        """

        writeable = 'w' in mode or 'a' in mode or '+' in mode
        fs, path = self.parse(fs_url, writeable=writeable)
        file_object = fs.open(path, mode)

        file_object = _FSClosingFile(fs, file_object, mode)
        #file_object.fs = fs
        return file_object

    def getcontents(self, fs_url, mode='rb', encoding=None, errors=None, newline=None):
        """Gets the contents from a given FS url (if it references a file)

        :param fs_url: a FS URL e.g. ftp://ftp.mozilla.org/README

        """
        fs, path = self.parse(fs_url)
        return fs.getcontents(path, mode, encoding=encoding, errors=errors, newline=newline)

    def opendir(self, fs_url, writeable=True, create_dir=False):
        """Opens an FS object from an FS URL

        :param fs_url: an FS URL e.g. ftp://ftp.mozilla.org
        :param writeable: set to True (the default) if the FS must be writeable
        :param create_dir: create the directory references by the FS URL, if
            it doesn't already exist

        """
        fs, path = self.parse(fs_url, writeable=writeable, create_dir=create_dir)
        if path and '://' not in fs_url:
            # A shortcut to return an OSFS rather than a SubFS for os paths
            return OSFS(fs_url)
        if path:
            fs = fs.opendir(path)
        return fs


class Opener(object):
    """The base class for openers

    Opener follow a very simple protocol. To create an opener, derive a class
    from `Opener` and define a classmethod called `get_fs`, which should have the following signature::

        @classmethod
        def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):

    The parameters of `get_fs` are as follows:

     * `fs_name` the name of the opener, as extracted from the protocol part of the url,
     * `fs_name_params` reserved for future use
     * `fs_path` the path part of the url
     * `writeable` if True, then `get_fs` must return an FS that can be written to
     * `create_dir` if True then `get_fs` should attempt to silently create the directory references in path

    In addition to `get_fs` an opener class should contain
    two class attributes: names and desc. `names` is a list of protocols that
    list opener will opener. `desc` is an English description of the individual opener syntax.

    """
    pass


class OSFSOpener(Opener):
    names = ['osfs', 'file']
    desc = """OS filesystem opener, works with any valid system path. This is the default opener and will be used if you don't indicate which opener to use.

    examples:
    * file://relative/foo/bar/baz.txt (opens a relative file)
    * file:///home/user (opens a directory from a absolute path)
    * osfs://~/ (open the user's home directory)
    * foo/bar.baz (file:// is the default opener)"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.osfs import OSFS

        path = os.path.normpath(fs_path)
        if create_dir and not os.path.exists(path):
            from fs.osfs import _os_makedirs
            _os_makedirs(path)
        dirname, resourcename = os.path.split(fs_path)
        osfs = OSFS(dirname)
        return osfs, resourcename

class ZipOpener(Opener):
    names = ['zip', 'zip64']
    desc = """Opens zip files. Use zip64 for > 2 gigabyte zip files, if you have a 64 bit processor.

    examples:
    * zip://myzip.zip (open a local zip file)
    * zip://myzip.zip!foo/bar/insidezip.txt (reference a file insize myzip.zip)
    * zip:ftp://ftp.example.org/myzip.zip (open a zip file stored on a ftp server)"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):

        zip_fs, zip_path = registry.parse(fs_path)
        if zip_path is None:
            raise OpenerError('File required for zip opener')
        if zip_fs.exists(zip_path):
            if writeable:
                open_mode = 'r+b'
            else:
                open_mode = 'rb'
        else:
            open_mode = 'w+'
        if zip_fs.hassyspath(zip_path):
            zip_file = zip_fs.getsyspath(zip_path)
        else:
            zip_file = zip_fs.open(zip_path, mode=open_mode)

        _username, _password, fs_path = _parse_credentials(fs_path)

        from fs.zipfs import ZipFS
        if zip_file is None:
            zip_file = fs_path

        mode = 'r'
        if writeable:
            mode = 'a'

        allow_zip_64 = fs_name.endswith('64')

        zipfs = ZipFS(zip_file, mode=mode, allow_zip_64=allow_zip_64)
        return zipfs, None


class RPCOpener(Opener):
    names = ['rpc']
    desc = """An opener for filesystems server over RPC (see the fsserve command).

examples:
rpc://127.0.0.1:8000 (opens a RPC server running on local host, port 80)
rpc://www.example.org (opens an RPC server on www.example.org, default port 80)"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.rpcfs import RPCFS
        _username, _password, fs_path = _parse_credentials(fs_path)
        if '://' not in fs_path:
            fs_path = 'http://' + fs_path

        scheme, netloc, path, _params, _query, _fragment = urlparse(fs_path)

        rpcfs = RPCFS('%s://%s' % (scheme, netloc))

        if create_dir and path:
            rpcfs.makedir(path, recursive=True, allow_recreate=True)

        return rpcfs, path or None


class FTPOpener(Opener):
    names = ['ftp']
    desc = """An opener for FTP (File Transfer Protocl) server

examples:
* ftp://ftp.mozilla.org (opens the root of ftp.mozilla.org)
* ftp://ftp.example.org/foo/bar (opens /foo/bar on ftp.mozilla.org)"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.ftpfs import FTPFS
        username, password, fs_path = _parse_credentials(fs_path)

        scheme, _netloc, _path, _params, _query, _fragment = urlparse(fs_path)
        if not scheme:
            fs_path = 'ftp://' + fs_path
        scheme, netloc, path, _params, _query, _fragment = urlparse(fs_path)

        dirpath, resourcepath = pathsplit(path)
        url = netloc

        ftpfs = FTPFS(url, user=username or '', passwd=password or '', follow_symlinks=(fs_name_params == "symlinks"))
        ftpfs.cache_hint(True)

        if create_dir and path:
            ftpfs.makedir(path, recursive=True, allow_recreate=True)

        if dirpath:
            ftpfs = ftpfs.opendir(dirpath)

        if not resourcepath:
            return ftpfs, None
        else:
            return ftpfs, resourcepath


class SFTPOpener(Opener):
    names = ['sftp']
    desc = """An opener for SFTP (Secure File Transfer Protocol) servers

examples:
* sftp://username:password@example.org (opens sftp server example.org with username and password
* sftp://example.org (opens example.org with public key authentication)"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create_dir):
        username, password, fs_path = _parse_credentials(fs_path)

        from fs.sftpfs import SFTPFS

        credentials = {}
        if username is not None:
            credentials['username'] = username
        if password is not None:
            credentials['password'] = password

        if '/' in fs_path:
            addr, fs_path = fs_path.split('/', 1)
        else:
            addr = fs_path
            fs_path = '/'

        fs_path, resourcename = pathsplit(fs_path)

        host = addr
        port = None
        if ':' in host:
            addr, port = host.rsplit(':', 1)
            try:
                port = int(port)
            except ValueError:
                pass
            else:
                host = (addr, port)

        if create_dir:
            sftpfs = SFTPFS(host, root_path='/', **credentials)
            if not sftpfs._transport.is_authenticated():
                sftpfs.close()
                raise OpenerError('SFTP requires authentication')
            sftpfs = sftpfs.makeopendir(fs_path)
            return sftpfs, None

        sftpfs = SFTPFS(host, root_path=fs_path, **credentials)
        if not sftpfs._transport.is_authenticated():
            sftpfs.close()
            raise OpenerError('SFTP requires authentication')

        return sftpfs, resourcename


class MemOpener(Opener):
    names = ['mem', 'ram']
    desc = """Creates an in-memory filesystem (very fast but contents will disappear on exit).
Useful for creating a fast temporary filesystem for serving or mounting with fsserve or fsmount.
NB: If you user fscp or fsmv to copy/move files here, you are effectively deleting them!

examples:
* mem:// (opens a new memory filesystem)
* mem://foo/bar (opens a new memory filesystem with subdirectory /foo/bar)    """

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create_dir):
        from fs.memoryfs import MemoryFS
        memfs = MemoryFS()
        if create_dir:
            memfs = memfs.makeopendir(fs_path)
        return memfs, None


class DebugOpener(Opener):
    names = ['debug']
    desc = """For developers -- adds debugging information to output.

example:
    * debug:ftp://ftp.mozilla.org (displays details of calls made to a ftp filesystem)"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create_dir):
        from fs.wrapfs.debugfs import DebugFS
        if fs_path:
            fs, _path = registry.parse(fs_path, writeable=writeable, create_dir=create_dir)
            return DebugFS(fs, verbose=False), None
        if fs_name_params == 'ram':
            from fs.memoryfs import MemoryFS
            return DebugFS(MemoryFS(), identifier=fs_name_params, verbose=False), None
        else:
            from fs.tempfs import TempFS
            return DebugFS(TempFS(), identifier=fs_name_params, verbose=False), None


class TempOpener(Opener):
    names = ['temp']
    desc = """Creates a temporary filesystem that is erased on exit.
Probably only useful for mounting or serving.
NB: If you use fscp or fsmv to copy/move files here, you are effectively deleting them!

example:
* temp://"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create_dir):
        from fs.tempfs import TempFS
        from fs.wrapfs.lazyfs import LazyFS
        fs = LazyFS((TempFS,(),{"identifier":fs_name_params}))
        return fs, fs_path


class S3Opener(Opener):
    names = ['s3']
    desc = """Opens a filesystem stored on Amazon S3 storage
    The environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY should be set"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.s3fs import S3FS

        username, password, bucket = _parse_credentials(fs_path)
        path = ''
        if '/' in bucket:
            bucket, path = fs_path.split('/', 1)

        fs = S3FS(bucket,
                  aws_access_key=username or None,
                  aws_secret_key=password or None)

        if path:
            dirpath, resourcepath = pathsplit(path)
            if dirpath:
                fs = fs.opendir(dirpath)
            path = resourcepath

        return fs, path


class TahoeOpener(Opener):
    names = ['tahoe']
    desc = """Opens a Tahoe-LAFS filesystem

    example:
    * tahoe://http://pubgrid.tahoe-lafs.org/uri/URI:DIR2:h5bkxelehowscijdb [...]"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.contrib.tahoelafs import TahoeLAFS

        if '/uri/' not in fs_path:
            raise OpenerError("""Tahoe-LAFS url should be in the form <url>/uri/<dicap>""")

        url, dircap = fs_path.split('/uri/')
        path = ''
        if '/' in dircap:
            dircap, path = dircap.split('/', 1)

        fs = TahoeLAFS(dircap, webapi=url)

        if '/' in path:
            dirname, _resourcename = pathsplit(path)
            if create_dir:
                fs = fs.makeopendir(dirname)
            else:
                fs = fs.opendir(dirname)
            path = ''

        return fs, path


class DavOpener(Opener):
    names = ['dav']
    desc = """Opens a WebDAV server

example:
* dav://example.org/dav"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.contrib.davfs import DAVFS

        url = fs_path

        if '://' not in url:
            url = 'http://' + url

        scheme, url = url.split('://', 1)

        username, password, url = _parse_credentials(url)

        credentials = None
        if username or password:
            credentials = {}
            if username:
                credentials['username'] = username
            if password:
                credentials['password'] = password

        url = '%s://%s' % (scheme, url)

        fs = DAVFS(url, credentials=credentials)

        return fs, ''

class HTTPOpener(Opener):
    names = ['http', 'https']
    desc = """HTTP file opener. HTTP only supports reading files, and not much else.

example:
* http://www.example.org/index.html"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        from fs.httpfs import HTTPFS
        if '/' in fs_path:
            dirname, resourcename = fs_path.rsplit('/', 1)
        else:
            dirname = fs_path
            resourcename = ''
        fs = HTTPFS('http://' + dirname)
        return fs, resourcename

class UserDataOpener(Opener):
    names = ['appuserdata', 'appuser']
    desc = """Opens a filesystem for a per-user application directory.

The 'domain' should be in the form <author name>:<application name>.<version> (the author name and version are optional).

example:
* appuserdata://myapplication
* appuserdata://examplesoft:myapplication
* appuserdata://anotherapp.1.1
* appuserdata://examplesoft:anotherapp.1.3"""

    FSClass = 'UserDataFS'

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):
        import fs.appdirfs
        fs_class = getattr(fs.appdirfs, cls.FSClass)
        if ':' in fs_path:
            appauthor, appname = fs_path.split(':', 1)
        else:
            appauthor = None
            appname = fs_path

        if '/' in appname:
            appname, path = appname.split('/', 1)
        else:
            path = ''

        if '.' in appname:
            appname, appversion = appname.split('.', 1)
        else:
            appversion = None

        fs = fs_class(appname, appauthor=appauthor, version=appversion, create=create_dir)

        if '/' in path:
            subdir, path = path.rsplit('/', 1)
            if create_dir:
                fs = fs.makeopendir(subdir, recursive=True)
            else:
                fs = fs.opendir(subdir)

        return fs, path

class SiteDataOpener(UserDataOpener):
    names = ['appsitedata', 'appsite']

    desc = """Opens a filesystem for an application site data directory.

The 'domain' should be in the form <author name>:<application name>.<version> (the author name and version are optional).

example:
* appsitedata://myapplication
* appsitedata://examplesoft:myapplication
* appsitedata://anotherapp.1.1
* appsitedata://examplesoft:anotherapp.1.3"""

    FSClass = 'SiteDataFS'

class UserCacheOpener(UserDataOpener):
    names = ['appusercache', 'appcache']

    desc = """Opens a filesystem for an per-user application cache directory.

The 'domain' should be in the form <author name>:<application name>.<version> (the author name and version are optional).

example:
* appusercache://myapplication
* appusercache://examplesoft:myapplication
* appusercache://anotherapp.1.1
* appusercache://examplesoft:anotherapp.1.3"""

    FSClass = 'UserCacheFS'


class UserLogOpener(UserDataOpener):
    names = ['appuserlog', 'applog']

    desc = """Opens a filesystem for an application site data directory.

The 'domain' should be in the form <author name>:<application name>.<version> (the author name and version are optional).

example:
* appuserlog://myapplication
* appuserlog://examplesoft:myapplication
* appuserlog://anotherapp.1.1
* appuserlog://examplesoft:anotherapp.1.3"""

    FSClass = 'UserLogFS'


class MountOpener(Opener):
    names = ['mount']
    desc = """Mounts other filesystems on a 'virtual' filesystem

The path portion of the FS URL should be a path to an ini file, where the keys are the mount point, and the values are FS URLs to mount.

The following is an example of such an ini file:

    [fs]
    resources=appuser://myapp/resources
    foo=~/foo
    foo/bar=mem://

    [fs2]
    bar=~/bar

example:
* mount://fs.ini
* mount://fs.ini!resources
* mount://fs.ini:fs2"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):

        from fs.mountfs import MountFS
        from ConfigParser import ConfigParser
        cfg = ConfigParser()

        if '#' in fs_path:
            path, section = fs_path.split('#', 1)
        else:
            path = fs_path
            section = 'fs'

        cfg.readfp(registry.open(path))

        mount_fs = MountFS()
        for mount_point, mount_path in cfg.items(section):
            mount_fs.mount(mount_point, registry.opendir(mount_path, create_dir=create_dir))
        return mount_fs, ''


class MultiOpener(Opener):
    names = ['multi']
    desc = """Combines other filesystems in to a single filesystem.

The path portion of the FS URL should be a path to an ini file, where the keys are the mount point, and the values are FS URLs to mount.

The following is an example of such an ini file:

    [templates]
    dir1=templates/foo
    dir2=templates/bar

example:
* multi://fs.ini"""

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create_dir):

        from fs.multifs import MultiFS
        from ConfigParser import ConfigParser
        cfg = ConfigParser()

        if '#' in fs_path:
            path, section = fs_path.split('#', 1)
        else:
            path = fs_path
            section = 'fs'

        cfg.readfp(registry.open(path))

        multi_fs = MultiFS()
        for name, fs_url in cfg.items(section):
            multi_fs.addfs(name, registry.opendir(fs_url, create_dir=create_dir))
        return multi_fs, ''


opener = OpenerRegistry([OSFSOpener,
                         ZipOpener,
                         RPCOpener,
                         FTPOpener,
                         SFTPOpener,
                         MemOpener,
                         DebugOpener,
                         TempOpener,
                         S3Opener,
                         TahoeOpener,
                         DavOpener,
                         HTTPOpener,
                         UserDataOpener,
                         SiteDataOpener,
                         UserCacheOpener,
                         UserLogOpener,
                         MountOpener,
                         MultiOpener
                         ])

fsopen = opener.open
fsopendir = opener.opendir

