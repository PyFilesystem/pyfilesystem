'''
fs.contrib.tahoelafs
====================

This modules provides a PyFilesystem interface to the Tahoe Least Authority
File System. Tahoe-LAFS is a distributed, encrypted, fault-tolerant storage
system:

    http://tahoe-lafs.org/

You will need access to a Tahoe-LAFS "web api" service.

Example (it will use publicly available (but slow) Tahoe-LAFS cloud)::

    from fs.contrib.tahoelafs import TahoeLAFS, Connection
    dircap = TahoeLAFS.createdircap(webapi='http://insecure.tahoe-lafs.org')
    print "Your dircap (unique key to your storage directory) is", dircap
    print "Keep it safe!"
    fs = TahoeLAFS(dircap, autorun=False, webapi='http://insecure.tahoe-lafs.org')
    f = fs.open("foo.txt", "a")
    f.write('bar!')
    f.close()
    print "Now visit %s and enjoy :-)" % fs.getpathurl('foo.txt')

When any problem occurred, you can turn on internal debugging messages::

    import logging    
    l = logging.getLogger()
    l.setLevel(logging.DEBUG)
    l.addHandler(logging.StreamHandler(sys.stdout))

    ... your Python code using TahoeLAFS ...
    
TODO:

   * unicode support
   * try network errors / bad happiness
   * exceptions
   * tests    
   * sanitize all path types (., /)
   * support for extra large file uploads (poster module)
   * Possibility to block write until upload done (Tahoe mailing list)
   * Report something sane when Tahoe crashed/unavailable
   * solve failed unit tests (makedir_winner, ...)
   * file times
   * docs & author
   * python3 support
   * remove creating blank files (depends on FileUploadManager)
   
TODO (Not TahoeLAFS specific tasks):
   * RemoteFileBuffer on the fly buffering support
   * RemoteFileBuffer unit tests
   * RemoteFileBuffer submit to trunk
   * Implement FileUploadManager + faking isfile/exists of just processing file
   * pyfilesystem docs is outdated (rename, movedir, ...)  

'''


import stat as statinfo

import logging
from logging import DEBUG, INFO, ERROR, CRITICAL

import fs
import fs.errors as errors
from fs.path import abspath, relpath, normpath, dirname, pathjoin
from fs.base import FS, NullFile
from fs import _thread_synchronize_default, SEEK_END
from fs.remote import CacheFSMixin, RemoteFileBuffer
from fs.base import fnmatch, NoDefaultMeta

from util import TahoeUtil
from connection import Connection   

from six import b

logger = fs.getLogger('fs.tahoelafs')

def _fix_path(func):
    """Method decorator for automatically normalising paths."""
    def wrapper(self, *args, **kwds):
        if len(args):
            args = list(args)
            args[0] = _fixpath(args[0])
        return func(self, *args, **kwds)
    return wrapper


def _fixpath(path):
    """Normalize the given path."""
    return abspath(normpath(path))
    
     

class _TahoeLAFS(FS):
    """FS providing raw access to a Tahoe-LAFS Filesystem.

    This class implements all the details of interacting with a Tahoe-backed
    filesystem, but you probably don't want to use it in practice.  Use the
    TahoeLAFS class instead, which has some internal caching to improve
    performance.
    """
    
    _meta = { 'virtual' : False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'network' : True
             }
        

    def __init__(self, dircap, largefilesize=10*1024*1024, webapi='http://127.0.0.1:3456'):
        '''Creates instance of TahoeLAFS.
            
            :param dircap: special hash allowing user to work with TahoeLAFS directory.
            :param largefilesize: - Create placeholder file for files larger than this treshold.
                Uploading and processing of large files can last extremely long (many hours),
                so placing this placeholder can help you to remember that upload is processing.
                Setting this to None will skip creating placeholder files for any uploads.
        '''
        self.dircap = dircap if not dircap.endswith('/') else dircap[:-1]
        self.largefilesize = largefilesize
        self.connection = Connection(webapi)
        self.tahoeutil = TahoeUtil(webapi)
        super(_TahoeLAFS, self).__init__(thread_synchronize=_thread_synchronize_default)       
        
    def __str__(self):
        return "<TahoeLAFS: %s>" % self.dircap 
    
    @classmethod
    def createdircap(cls, webapi='http://127.0.0.1:3456'):
        return TahoeUtil(webapi).createdircap()

    def getmeta(self,meta_name,default=NoDefaultMeta):
        if meta_name == "read_only":
            return self.dircap.startswith('URI:DIR2-RO')
        return super(_TahoeLAFS,self).getmeta(meta_name,default)
    
    @_fix_path
    def open(self, path, mode='r', **kwargs):
        self._log(INFO, 'Opening file %s in mode %s' % (path, mode))        
        newfile = False
        if not self.exists(path):
            if 'w' in mode or 'a' in mode:
                newfile = True
            else:
                self._log(DEBUG, "File %s not found while opening for reads" % path)
                raise errors.ResourceNotFoundError(path)
        elif self.isdir(path):
            self._log(DEBUG, "Path %s is directory, not a file" % path)
            raise errors.ResourceInvalidError(path)
        elif 'w' in mode:
            newfile = True
        
        if newfile:
            self._log(DEBUG, 'Creating empty file %s' % path)
            if self.getmeta("read_only"):
                raise errors.UnsupportedError('read only filesystem')
            self.setcontents(path, b(''))
            handler = NullFile()
        else:
            self._log(DEBUG, 'Opening existing file %s for reading' % path)
            handler = self.getrange(path,0)
        
        return RemoteFileBuffer(self, path, mode, handler,
                    write_on_flush=False)

    @_fix_path
    def desc(self, path):
        try:
            return self.getinfo(path)
        except:
            return ''
    
    @_fix_path
    def exists(self, path):
        try:
            self.getinfo(path)
            self._log(DEBUG, "Path %s exists" % path)
            return True
        except errors.ResourceNotFoundError:
            self._log(DEBUG, "Path %s does not exists" % path)
            return False
        except errors.ResourceInvalidError:
            self._log(DEBUG, "Path %s does not exists, probably misspelled URI" % path)
            return False
     
    @_fix_path
    def getsize(self, path):
        try:
            size = self.getinfo(path)['size']
            self._log(DEBUG, "Size of %s is %d" % (path, size))
            return size
        except errors.ResourceNotFoundError:
            return 0
    
    @_fix_path
    def isfile(self, path):
        try:
            isfile = (self.getinfo(path)['type'] == 'filenode')
        except errors.ResourceNotFoundError:
            #isfile = not path.endswith('/')
            isfile = False
        self._log(DEBUG, "Path %s is file: %d" % (path, isfile))
        return isfile
    
    @_fix_path        
    def isdir(self, path):
        try:
            isdir = (self.getinfo(path)['type'] == 'dirnode')
        except errors.ResourceNotFoundError:
            isdir = False
        self._log(DEBUG, "Path %s is directory: %d" % (path, isdir))
        return isdir

    
    def listdir(self, *args, **kwargs):
        return [ item[0] for item in self.listdirinfo(*args, **kwargs) ]        

    def listdirinfo(self, *args, **kwds):
        return list(self.ilistdirinfo(*args,**kwds))

    def ilistdir(self, *args, **kwds):
        for item in self.ilistdirinfo(*args,**kwds):
            yield item[0]
    
    @_fix_path
    def ilistdirinfo(self, path="/", wildcard=None, full=False, absolute=False,
                    dirs_only=False, files_only=False):
        self._log(DEBUG, "Listing directory (listdirinfo) %s" % path)
        
        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only can not both be True")
        
        for item in self.tahoeutil.list(self.dircap, path):
            if dirs_only and item['type'] == 'filenode':
                continue
            elif files_only and item['type'] == 'dirnode':
                continue
            
            if wildcard is not None:
                if isinstance(wildcard,basestring):
                    if not fnmatch.fnmatch(item['name'], wildcard):
                        continue
                else:
                    if not wildcard(item['name']):
                        continue
            
            if full:
                item_path = relpath(pathjoin(path, item['name']))
            elif absolute:
                item_path = abspath(pathjoin(path, item['name']))    
            else:
                item_path = item['name']
            
            yield (item_path, item)
     
    @_fix_path
    def remove(self, path):
        self._log(INFO, 'Removing file %s' % path)
        if self.getmeta("read_only"):
            raise errors.UnsupportedError('read only filesystem')

        if not self.isfile(path):
            if not self.isdir(path):
                raise errors.ResourceNotFoundError(path)
            raise errors.ResourceInvalidError(path)
        
        try:
            self.tahoeutil.unlink(self.dircap, path)
        except Exception, e:
            raise errors.ResourceInvalidError(path)
    
    @_fix_path
    def removedir(self, path, recursive=False, force=False):
        self._log(INFO, "Removing directory %s" % path) 
        if self.getmeta("read_only"):
            raise errors.UnsupportedError('read only filesystem')
        if not self.isdir(path):
            if not self.isfile(path):
                raise errors.ResourceNotFoundError(path)
            raise errors.ResourceInvalidError(path)
        if not force and self.listdir(path):
            raise errors.DirectoryNotEmptyError(path)
        
        self.tahoeutil.unlink(self.dircap, path)

        if recursive and path != '/':
            try:
                self.removedir(dirname(path), recursive=True)
            except errors.DirectoryNotEmptyError:
                pass
    
    @_fix_path
    def makedir(self, path, recursive=False, allow_recreate=False):
        self._log(INFO, "Creating directory %s" % path)
        if self.getmeta("read_only"):
            raise errors.UnsupportedError('read only filesystem')       
        if self.exists(path):
            if not self.isdir(path):
                raise errors.ResourceInvalidError(path)
            if not allow_recreate: 
                raise errors.DestinationExistsError(path)
        if not recursive and not self.exists(dirname(path)):
            raise errors.ParentDirectoryMissingError(path)
        self.tahoeutil.mkdir(self.dircap, path)
        
    def movedir(self, src, dst, overwrite=False):
        self.move(src, dst, overwrite=overwrite)
    
    def move(self, src, dst, overwrite=False):
        self._log(INFO, "Moving file from %s to %s" % (src, dst))
        if self.getmeta("read_only"):
            raise errors.UnsupportedError('read only filesystem')
        src = _fixpath(src)
        dst = _fixpath(dst)
        if not self.exists(dirname(dst)):
            raise errors.ParentDirectoryMissingError(dst)
        if not overwrite and self.exists(dst):
            raise errors.DestinationExistsError(dst)
        self.tahoeutil.move(self.dircap, src, dst)

    def rename(self, src, dst):
        self.move(src, dst)
        
    def copy(self, src, dst, overwrite=False, chunk_size=16384):
        if self.getmeta("read_only"):
            raise errors.UnsupportedError('read only filesystem')
        # FIXME: this is out of date; how to do native tahoe copy?
        # FIXME: Workaround because isfile() not exists on _TahoeLAFS
        FS.copy(self, src, dst, overwrite, chunk_size)
        
    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        if self.getmeta("read_only"):
            raise errors.UnsupportedError('read only filesystem')
        # FIXME: this is out of date; how to do native tahoe copy?
        # FIXME: Workaround because isfile() not exists on _TahoeLAFS
        FS.copydir(self, src, dst, overwrite, ignore_errors, chunk_size)
       
    
    def _log(self, level, message):
        if not logger.isEnabledFor(level): return
        logger.log(level, u'(%d) %s' % (id(self),
                                unicode(message).encode('ASCII', 'replace')))
        
    @_fix_path
    def getpathurl(self, path, allow_none=False, webapi=None):
        '''
            Retrieve URL where the file/directory is stored
        '''
        if webapi == None:
            webapi = self.connection.webapi
        self._log(DEBUG, "Retrieving URL for %s over %s" % (path, webapi))
        path = self.tahoeutil.fixwinpath(path, False)
        return u"%s/uri/%s%s" % (webapi, self.dircap, path)

    @_fix_path
    def getrange(self, path, offset, length=None):
        return self.connection.get(u'/uri/%s%s' % (self.dircap, path),
                    offset=offset, length=length)
       
    @_fix_path             
    def setcontents(self, path, file, chunk_size=64*1024):    
        self._log(INFO, 'Uploading file %s' % path)
        size=None
        
        if self.getmeta("read_only"):
            raise errors.UnsupportedError('read only filesystem')
        
        # Workaround for large files:
        # First create zero file placeholder, then
        # upload final content.
        if self.largefilesize != None and getattr(file, 'read', None):
            # As 'file' can be also a string, need to check,
            # if 'file' looks like duck. Sorry, file.
            file.seek(0, SEEK_END)
            size = file.tell()
            file.seek(0)

            if size > self.largefilesize:
                self.connection.put(u'/uri/%s%s' % (self.dircap, path),
                    "PyFilesystem.TahoeLAFS: Upload started, final size %d" % size)

        self.connection.put(u'/uri/%s%s' % (self.dircap, path), file, size=size)

    @_fix_path
    def getinfo(self, path): 
        self._log(INFO, 'Reading meta for %s' % path)
        info = self.tahoeutil.info(self.dircap, path)        
        #import datetime
        #info['created_time'] = datetime.datetime.now()
        #info['modified_time'] = datetime.datetime.now()
        #info['accessed_time'] = datetime.datetime.now()
        if info['type'] == 'filenode':
            info["st_mode"] = 0x700 | statinfo.S_IFREG
        elif info['type'] == 'dirnode':
            info["st_mode"] = 0x700 | statinfo.S_IFDIR
        return info



class TahoeLAFS(CacheFSMixin,_TahoeLAFS):
    """FS providing cached access to a Tahoe Filesystem.

    This class is the preferred means to access a Tahoe filesystem.  It
    maintains an internal cache of recently-accessed metadata to speed
    up operations.
    """

    def __init__(self, *args, **kwds):
        kwds.setdefault("cache_timeout",60)
        super(TahoeLAFS,self).__init__(*args,**kwds)


