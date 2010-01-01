"""
fs.sftpfs
=========

Filesystem accessing an SFTP server (via paramiko)

"""

import datetime 
import stat as statinfo

import paramiko

from fs.base import *

# SFTPClient appears to not be thread-safe, so we use an instance per thread
if hasattr(threading,"local"):
    thread_local = threading.local
else:
    class thread_local(object):
        def __init__(self):
            self._map = {}
        def __getattr__(self,attr):
            try:
                return self._map[(threading.currentThread().ident,attr)]
            except KeyError:
                raise AttributeError, attr
        def __setattr__(self,attr,value):
            self._map[(threading.currentThread().ident,attr)] = value



if not hasattr(paramiko.SFTPFile,"__enter__"):
    paramiko.SFTPFile.__enter__ = lambda self: self
    paramiko.SFTPFile.__exit__ = lambda self,et,ev,tb: self.close() and False


class SFTPFS(FS):
    """A filesystem stored on a remote SFTP server.

    This is basically a compatability wrapper for the excellent SFTPClient
    class in the paramiko module.
    """

    def __init__(self,connection,root_path="/",encoding=None,**credentials):
        """SFTPFS constructor.

        The only required argument is 'connection', which must be something
        from which we can construct a paramiko.SFTPClient object.  Possibile
        values include:

            * a hostname string
            * a (hostname,port) tuple
            * a paramiko.Transport instance
            * a paramiko.Channel instance in "sftp" mode

        The kwd argument 'root_path' specifies the root directory on the remote
        machine - access to files outsite this root wil be prevented. Any
        other keyword arguments are assumed to be credentials to be used when
        connecting the transport.
        """
        if encoding is None:
            encoding = "utf8"
        self.encoding = encoding
        self.closed = False
        self._owns_transport = False
        self._credentials = credentials
        self._tlocal = thread_local()
        self._transport = None
        self._client = None
        if isinstance(connection,paramiko.Channel):
            self._transport = None
            self._client = paramiko.SFTPClient(connection)
        else:
            if not isinstance(connection,paramiko.Transport):
                connection = paramiko.Transport(connection)
                self._owns_transport = True
            if not connection.is_authenticated():
                connection.connect(**credentials)
            self._transport = connection
        self.root_path = abspath(normpath(root_path))

    def __del__(self):
        self.close()

    def __getstate__(self):
        state = super(SFTPFS,self).__getstate__()
        del state["_tlocal"]
        if self._owns_transport:
            state['_transport'] = self._transport.getpeername()
        return state

    def __setstate__(self,state):
        for (k,v) in state.iteritems():
            self.__dict__[k] = v
        self._tlocal = thread_local()
        if self._owns_transport:
            self._transport = paramiko.Transport(self._transport)
            self._transport.connect(**self._credentials)

    @property
    def client(self):
        try:
            return self._tlocal.client
        except AttributeError:
            if self._transport is None:
                return self._client
            client = paramiko.SFTPClient.from_transport(self._transport)
            self._tlocal.client = client
            return client

    def close(self):
        """Close the connection to the remote server."""
        if not self.closed:
            if self.client:
                self.client.close()
            if self._owns_transport and self._transport:
                self._transport.close()

    def _normpath(self,path):
        if not isinstance(path,unicode):
            path = path.decode(self.encoding)
        npath = pathjoin(self.root_path,relpath(normpath(path)))
        if not isprefix(self.root_path,npath):
            raise PathError(path,msg="Path is outside root: %(path)s")
        return npath

    @convert_os_errors
    def open(self,path,mode="r",bufsize=-1):
        npath = self._normpath(path)
        #  paramiko implements its own buffering and write-back logic,
        #  so we don't need to use a RemoteFileBuffer here.
        f = self.client.open(npath,mode,bufsize)
        if self.isdir(path):
            msg = "that's a directory: %(path)s"
            raise ResourceInvalidError(path,msg=msg)
        return f

    @convert_os_errors
    def exists(self,path):
        npath = self._normpath(path)
        try:
            self.client.stat(npath)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                return False
            raise
        return True
        
    @convert_os_errors
    def isdir(self,path):
        npath = self._normpath(path)
        try:
            stat = self.client.stat(npath)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                return False
            raise
        return statinfo.S_ISDIR(stat.st_mode)

    @convert_os_errors
    def isfile(self,path):
        npath = self._normpath(path)
        try:
            stat = self.client.stat(npath)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                return False
            raise
        return statinfo.S_ISREG(stat.st_mode)

    @convert_os_errors
    def listdir(self,path="./",wildcard=None,full=False,absolute=False,dirs_only=False,files_only=False):
        npath = self._normpath(path)
        try:
            paths = self.client.listdir(npath)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                if self.isfile(path):
                    raise ResourceInvalidError(path,msg="Can't list directory contents of a file: %(path)s")
                raise ResourceNotFoundError(path)
            elif self.isfile(path):
                raise ResourceInvalidError(path,msg="Can't list directory contents of a file: %(path)s")
            raise
        for (i,p) in enumerate(paths):
            if not isinstance(p,unicode):
                paths[i] = p.decode(self.encoding)
        return self._listdir_helper(path, paths, wildcard, full, absolute, dirs_only, files_only)

    @convert_os_errors
    def makedir(self,path,recursive=False,allow_recreate=False):
        npath = self._normpath(path)
        try:
            self.client.mkdir(npath)
        except IOError, e:
            # Error code is unreliable, try to figure out what went wrong
            try:
                stat = self.client.stat(npath)
            except IOError:
                if not self.isdir(dirname(path)):
                    # Parent dir is missing
                    if not recursive:
                        raise ParentDirectoryMissingError(path)
                    self.makedir(dirname(path),recursive=True)
                    self.makedir(path,allow_recreate=allow_recreate)
                else:
                    # Undetermined error, let the decorator handle it
                    raise 
            else:
                # Destination exists
                if statinfo.S_ISDIR(stat.st_mode):
                    if not allow_recreate:
                        raise DestinationExistsError(path,msg="Can't create a directory that already exists (try allow_recreate=True): %(path)s")
                else:
                    raise ResourceInvalidError(path,msg="Can't create directory, there's already a file of that name: %(path)s")

    @convert_os_errors
    def remove(self,path):
        npath = self._normpath(path)
        try:
            self.client.remove(npath)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                raise ResourceNotFoundError(path)
            elif self.isdir(path):
                raise ResourceInvalidError(path,msg="Cannot use remove() on a directory: %(path)s")
            raise

    @convert_os_errors
    def removedir(self,path,recursive=False,force=False):
        npath = self._normpath(path)
        if path in ("","/"):
            return
        if force:
            for path2 in self.listdir(path,absolute=True):
                try:
                    self.remove(path2)
                except ResourceInvalidError:
                    self.removedir(path2,force=True)
        try:
            self.client.rmdir(npath)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                if self.isfile(path):
                    raise ResourceInvalidError(path,msg="Can't use removedir() on a file: %(path)s")
                raise ResourceNotFoundError(path)
            elif self.listdir(path):
                raise DirectoryNotEmptyError(path)
            raise
        if recursive:
            try:
                self.removedir(dirname(path),recursive=True)
            except DirectoryNotEmptyError:
                pass

    @convert_os_errors
    def rename(self,src,dst):
        nsrc = self._normpath(src)
        ndst = self._normpath(dst)
        try:
            self.client.rename(nsrc,ndst)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                raise ResourceNotFoundError(path)
            raise

    @convert_os_errors
    def move(self,src,dst,overwrite=False,chunk_size=16384):
        nsrc = self._normpath(src)
        ndst = self._normpath(dst)
        if overwrite and self.isfile(dst):
            self.remove(dst)
        try:
            self.client.rename(nsrc,ndst)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                raise ResourceNotFoundError(path)
            if self.exists(dst):
                raise DestinationExistsError(dst)
            if not self.isdir(dirname(dst)):
                raise ParentDirectoryMissingError(dst,msg="Destination directory does not exist: %(path)s")
            raise

    @convert_os_errors
    def movedir(self,src,dst,overwrite=False,ignore_errors=False,chunk_size=16384):
        nsrc = self._normpath(src)
        ndst = self._normpath(dst)
        if overwrite and self.isdir(dst):
            self.removedir(dst)
        try:
            self.client.rename(nsrc,ndst)
        except IOError, e:
            if getattr(e,"errno",None) == 2:
                raise ResourceNotFoundError(path)
            if self.exists(dst):
                raise DestinationExistsError(dst)
            if not self.isdir(dirname(dst)):
                raise ParentDirectoryMissingError(dst,msg="Destination directory does not exist: %(path)s")
            raise

    @convert_os_errors
    def getinfo(self, path):
        npath = self._normpath(path)
        stats = self.client.stat(npath)
        info = dict((k, getattr(stats, k)) for k in dir(stats) if not k.startswith('__') )
        info['size'] = info['st_size']
        ct = info.get('st_ctime', None)
        if ct is not None:
            info['created_time'] = datetime.datetime.fromtimestamp(ct)
        at = info.get('st_atime', None)
        if at is not None:
            info['accessed_time'] = datetime.datetime.fromtimestamp(at)
        mt = info.get('st_mtime', None)
        if mt is not None:
            info['modified_time'] = datetime.datetime.fromtimestamp(at)
        return info

    @convert_os_errors
    def getsize(self, path):
        npath = self._normpath(path)
        stats = self.client.stat(npath)
        return stats.st_size
 

