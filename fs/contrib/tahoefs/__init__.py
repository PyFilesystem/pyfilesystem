'''
Example (it will use publicly available, but slow-as-hell Tahoe-LAFS cloud):

    from fs.tahoefs import TahoeFS, Connection
    dircap = TahoeFS.createdircap(webapi='http://pubgrid.tahoe-lafs.org')
    print "Your dircap (unique key to your storage directory) is", dircap
    print "Keep it safe!"
    fs = TahoeFS(dircap, autorun=False, timeout=300, webapi='http://pubgrid.tahoe-lafs.org')
    f = fs.open("foo.txt", "a")
    f.write('bar!')
    f.close()
    print "Now visit %s and enjoy :-)" % fs.getpathurl('foo.txt')

When any problem occurred, you can turn on internal debugging messages:

    import logging    
    l = logging.getLogger()
    l.setLevel(logging.DEBUG)
    l.addHandler(logging.StreamHandler(sys.stdout))

    ... your Python code using TahoeFS ...
    
TODO:
   x unicode support
   x try network errors / bad happiness
   x colon hack (all occurences of ':' in filenames transparently convert to __colon__)
   x logging / cleaning sources from print()
   x exceptions
   x tests    
   x create ticket and send initial code
   x sanitize all path types (., /)
   x rewrite listdir, add listdirinfo
   x support for extra large file uploads (poster module)
   x Possibility to block write until upload done (Tahoe mailing list)
   x Report something sane when Tahoe crashed/unavailable
   x solve failed unit tests (makedir_winner, ...)
   filetimes
   docs & author
   python3 support
   python 2.3 support
   remove creating blank files (depends on FileUploadManager)
   
TODO (Not TahoeFS specific tasks):
   x DebugFS
   x RemoteFileBuffer on the fly buffering support
   x RemoteFileBuffer unit tests
   x RemoteFileBuffer submit to trunk
   colon hack -> move outside tahoe, should be in windows-specific FS (maybe in Dokan?)
   autorun hack -> move outside tahoe, -||-
   Implement FileUploadManager + faking isfile/exists of just processing file
   pyfilesystem docs is outdated (rename, movedir, ...)  
'''

import logging
from logging import DEBUG, INFO, ERROR, CRITICAL

import fs.errors as errors
from fs.path import abspath, relpath, normpath, dirname, pathjoin
from fs import FS, NullFile, _thread_synchronize_default, SEEK_END
from fs.remote import CacheFS, _cached_method, RemoteFileBuffer
from fs.base import fnmatch

from util import TahoeUtil
from connection import Connection   
#from .debugfs import DebugFS

logger = logging.getLogger('fs.tahoefs')

def _fix_path(func):
    def wrapper(self, *args, **kwds):
        if len(args):
            args = list(args)
            args[0] = abspath(normpath(args[0]))
        return func(self, *args, **kwds)
    return wrapper
     
class TahoeFS(CacheFS):
    
    _meta = { 'virtual' : False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'may_block' : False
             }
    
    def __init__(self, dircap, timeout=60, autorun=True, largefilesize=10*1024*1024, webapi='http://127.0.0.1:3456'):
        '''
            Creates instance of TahoeFS.
            dircap - special hash allowing user to work with TahoeLAFS directory.
            timeout - how long should underlying CacheFS keep information about files
                before asking TahoeLAFS node again.
            autorun - Allow listing autorun files? Can be very dangerous on Windows!.
                This is temporary hack, as it should be part of Windows-specific middleware,
                not Tahoe itself.
            largefilesize - Create placeholder file for files larger than this tresholf.
                Uploading and processing of large files can last extremely long (many hours),
                so placing this placeholder can help you to remember that upload is processing.
                Setting this to None will skip creating placeholder files for any uploads.
        '''
        fs = _TahoeFS(dircap, autorun=autorun, largefilesize=largefilesize, webapi=webapi)
        
        super(TahoeFS, self).__init__(fs, timeout)
        
    def __str__(self):
        return "<TahoeFS: %s>" % self.dircap 
    
    @classmethod
    def createdircap(cls, webapi='http://127.0.0.1:3456'):
        return TahoeUtil(webapi).createdircap()
    
    @_fix_path
    def open(self, path, mode='r', **kwargs):
        self.wrapped_fs._log(INFO, 'Opening file %s in mode %s' % (path, mode))        
        newfile = False
        if not self.exists(path):
            if 'w' in mode or 'a' in mode:
                newfile = True
            else:
                self.wrapped_fs._log(DEBUG, "File %s not found while opening for reads" % path)
                raise errors.ResourceNotFoundError(path)
            
        elif self.isdir(path):
            self.wrapped_fs._log(DEBUG, "Path %s is directory, not a file" % path)
            raise errors.ResourceInvalidError(path)
         
        if 'w' in mode:
            newfile = True
        
        if newfile:
            self.wrapped_fs._log(DEBUG, 'Creating empty file %s' % path)
            if self.wrapped_fs.readonly:          
                raise errors.UnsupportedError('read only filesystem')

            self.setcontents(path, '')
            handler = NullFile()
        else:
            self.wrapped_fs._log(DEBUG, 'Opening existing file %s for reading' % path)
            handler = self.wrapped_fs._get_file_handler(path)
        
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
            self.wrapped_fs._log(DEBUG, "Path %s exists" % path)
            return True
        except errors.ResourceNotFoundError:
            self.wrapped_fs._log(DEBUG, "Path %s does not exists" % path)
            return False
        except errors.ResourceInvalidError:
            self.wrapped_fs._log(DEBUG, "Path %s does not exists, probably misspelled URI" % path)
            return False
     
    @_fix_path
    def getsize(self, path):
        try:
            size = self.getinfo(path)['size']
            self.wrapped_fs._log(DEBUG, "Size of %s is %d" % (path, size))
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
        self.wrapped_fs._log(DEBUG, "Path %s is file: %d" % (path, isfile))
        return isfile
    
    @_fix_path        
    def isdir(self, path):
        try:
            isdir = (self.getinfo(path)['type'] == 'dirnode')
        except errors.ResourceNotFoundError:
            isdir = False
        self.wrapped_fs._log(DEBUG, "Path %s is directory: %d" % (path, isdir))
        return isdir

    
    @_fix_path
    @_cached_method
    def listdirinfo(self, path="/", wildcard=None, full=False, absolute=False,
                    dirs_only=False, files_only=False):
        self.wrapped_fs._log(DEBUG, "Listing directory (listdirinfo) %s" % path)
        _fixpath = self.wrapped_fs._fixpath
        _path = _fixpath(path)
        
        if dirs_only and files_only:
            raise errors.ValueError("dirs_only and files_only can not both be True")
        
        result = []
        for item in self.wrapped_fs.tahoeutil.list(self.dircap, _path):
            if dirs_only and item['type'] == 'filenode':
                continue
            elif files_only and item['type'] == 'dirnode':
                continue
            
            if wildcard is not None and \
               not fnmatch.fnmatch(item['name'], wildcard):
                continue
            
            if full:
                item_path = relpath(pathjoin(_path, item['name']))
            elif absolute:
                item_path = abspath(pathjoin(_path, item['name']))    
            else:
                item_path = item['name']
            
            cache_name = self.wrapped_fs._fixpath(u"%s/%s" % \
                                            (path, item['name'])) 
            self._cache_set(cache_name, 'getinfo', (), {}, (True, item))
            
            result.append((item_path, item))
            
        return result
     
    def listdir(self, *args, **kwargs):
        return [ item[0] for item in self.listdirinfo(*args, **kwargs) ]        
    
    @_fix_path
    def remove(self, path):
        self.wrapped_fs._log(INFO, 'Removing file %s' % path)
        if self.wrapped_fs.readonly:          
            raise errors.UnsupportedError('read only filesystem')

        if not self.isfile(path):
            if not self.isdir(path):
                raise errors.ResourceNotFoundError(path)
            raise errors.ResourceInvalidError(path)
        
        try:
            self.wrapped_fs.tahoeutil.unlink(self.dircap, path)
        except Exception, e:
            raise errors.ResourceInvalidError(path)
        finally:
            self._uncache(path, removed=True)
    
    @_fix_path
    def removedir(self, path, recursive=False, force=False):
        self.wrapped_fs._log(INFO, "Removing directory %s" % path) 
        if self.wrapped_fs.readonly:          
            raise errors.UnsupportedError('read only filesystem')
        if not self.isdir(path):
            if not self.isfile(path):
                raise errors.ResourceNotFoundError(path)
            raise errors.ResourceInvalidError(path)
        if not force and self.listdir(path):
            raise errors.DirectoryNotEmptyError(path)
        
        try:
            self.wrapped_fs.tahoeutil.unlink(self.dircap, path)
        finally:
            self._uncache(path, removed=True)

        if recursive and path != '/':
            try:
                self.removedir(dirname(path), recursive=True)
            except errors.DirectoryNotEmptyError:
                pass
    
    @_fix_path
    def makedir(self, path, recursive=False, allow_recreate=False):
        self.wrapped_fs._log(INFO, "Creating directory %s" % path)
        
        if self.wrapped_fs.readonly:          
            raise errors.UnsupportedError('read only filesystem')       
        if self.exists(path):
            if not self.isdir(path):
                raise errors.ResourceInvalidError(path)
            if not allow_recreate: 
                raise errors.DestinationExistsError(path)
        if not recursive and not self.exists(dirname(path)):
            raise errors.ParentDirectoryMissingError(path)
        
        try:
            self.wrapped_fs.tahoeutil.mkdir(self.dircap, path)
        finally:
            self._uncache(path,added=True)
        
    def movedir(self, src, dst, overwrite=False):
        self.move(src, dst, overwrite)
    
    def move(self, src, dst, overwrite=False):
        # FIXME: overwrite not documented
        self.wrapped_fs._log(INFO, "Moving file from %s to %s" % (src, dst))
        
        if self.wrapped_fs.readonly:          
            raise errors.UnsupportedError('read only filesystem')

        src = self.wrapped_fs._fixpath(src)
        dst = self.wrapped_fs._fixpath(dst)
        if not self.exists(dirname(dst)):
            # FIXME: Why raise exception when it is legal construct?
            raise errors.ParentDirectoryMissingError(dst)
        
        if not overwrite and self.exists(dst):
            raise errors.DestinationExistsError(dst)
        
        try:
            self.wrapped_fs.tahoeutil.move(self.dircap, src, dst)
        finally:
            self._uncache(src,removed=True)
            self._uncache(dst,added=True)

    @_fix_path
    def setcontents(self, path, file):
        try:
            self.wrapped_fs.setcontents(path, file)
        finally:
            self._uncache(path, added=True)

    def rename(self, src, dst):
        self.move(src, dst)
        
    def copy(self, src, dst, overwrite=False, chunk_size=16384):
        if self.wrapped_fs.readonly:          
            raise errors.UnsupportedError('read only filesystem')
        
        # FIXME: Workaround because isfile() not exists on _TahoeFS
        FS.copy(self, src, dst, overwrite, chunk_size)
        
    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        if self.wrapped_fs.readonly:          
            raise errors.UnsupportedError('read only filesystem')

        # FIXME: Workaround because isfile() not exists on _TahoeFS
        FS.copydir(self, src, dst, overwrite, ignore_errors, chunk_size)
       
class _TahoeFS(FS):    
    def __init__(self, dircap, autorun, largefilesize, webapi):
        self.dircap = dircap if not dircap.endswith('/') else dircap[:-1]
        self.autorun = autorun
        self.largefilesize = largefilesize
        self.connection = Connection(webapi)
        self.tahoeutil = TahoeUtil(webapi)
        self.readonly = dircap.startswith('URI:DIR2-RO')
        
        super(_TahoeFS, self).__init__(thread_synchronize=_thread_synchronize_default)       
    
    def _log(self, level, message):
        if not logger.isEnabledFor(level): return
        logger.log(level, u'(%d) %s' % (id(self),
                                unicode(message).encode('ASCII', 'replace')))
        
    def _fixpath(self, path):
        return abspath(normpath(path))
    
    def _get_file_handler(self, path):
        if not self.autorun:
            if path.lower().startswith('/autorun.'):
                self._log(DEBUG, 'Access to file %s denied' % path)
                return NullFile()

        return self.getrange(path, 0)
    
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
        path = self.tahoeutil.fixwinpath(path, False)
        return self.connection.get(u'/uri/%s%s' % (self.dircap, path),
                    offset=offset, length=length)
       
    @_fix_path             
    def setcontents(self, path, file):    
        self._log(INFO, 'Uploading file %s' % path)
        path = self.tahoeutil.fixwinpath(path, False)
        size=None
        
        if self.readonly:
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
                    "PyFilesystem.TahoeFS: Upload started, final size %d" % size)

        self.connection.put(u'/uri/%s%s' % (self.dircap, path), file, size=size)

    @_fix_path
    def getinfo(self, path): 
        self._log(INFO, 'Reading meta for %s' % path)
        info = self.tahoeutil.info(self.dircap, path)        
        #import datetime
        #info['created_time'] = datetime.datetime.now()
        #info['modified_time'] = datetime.datetime.now()
        #info['accessed_time'] = datetime.datetime.now()
        return info
