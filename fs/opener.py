import sys
from fs.osfs import OSFS
from fs.path import pathsplit, basename, join, iswildcard
import os
import os.path
import re
from urlparse import urlparse

class OpenerError(Exception):
    pass


class NoOpenerError(OpenerError):
    pass


class MissingParameterError(OpenerError):
    pass


def _expand_syspath(path):
    if path is None:
        return path      
    path = os.path.expanduser(os.path.expandvars(path))    
    path = os.path.normpath(os.path.abspath(path))    
    if sys.platform == "win32":
        if not path.startswith("\\\\?\\"):
            path = u"\\\\?\\" + root_path
        #  If it points at the root of a drive, it needs a trailing slash.
        if len(path) == 6:
            path = path + "\\"

    return path




class OpenerRegistry(object):
     

    re_fs_url = re.compile(r'''
^
(.*?)
:\/\/

(?:
\((.*?)\)
|(.*?)
)

(?:
\+(.*?)$
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
        if name not in self.registry:
            raise NoOpenerError("No opener for %s" % name)
        index = self.registry[name]
        return self.openers[index]        
    
    def add(self, opener):
        index = len(self.openers)
        self.openers[index] = opener
        for name in opener.names:
            self.registry[name] = index
    
    def parse(self, fs_url, default_fs_name=None, writeable=False, create=False):
                   
        orig_url = fs_url     
        match = self.split_segments(fs_url)
        
        if match:
            fs_name, paren_url, fs_url, path = match.groups()                                
            fs_url = fs_url or paren_url or ''          
            if ':' in fs_name:
                fs_name, sub_protocol = fs_name.split(':', 1)
                fs_url = '%s://%s' % (sub_protocol, fs_url)
            
            fs_name = fs_name or self.default_opener                                                                    
                
        else:
            fs_name = default_fs_name or self.default_opener
            fs_url = _expand_syspath(fs_url) 
            path = ''           

    
        fs_name,  fs_name_params = self.parse_name(fs_name)        
        opener = self.get_opener(fs_name)
        
        if fs_url is None:
            raise OpenerError("Unable to parse '%s'" % orig_url)
        
        wildcard = None
        if iswildcard(fs_url):
            fs_url, wildcard = pathsplit(fs_url)
        
        fs, fs_path = opener.get_fs(self, fs_name, fs_name_params, fs_url, writeable, create)
                
        if wildcard:
            fs_path = join(fs_path or '', wildcard)
        else:
            path = join(fs_path or '', path)
        
        if path:
            pathname, resourcename = pathsplit(path)
            if pathname:
                fs = fs.opendir(pathname)
                path = resourcename
            if not iswildcard(path):
                if fs.isdir(path):
                    fs = fs.opendir(path)
                    fs_path = ''
                else:
                    fs_path = path
                
        
        return fs, fs_path
    
    def parse_credentials(self, url):
        
        username = None
        password = None
        if '@' in url:
            credentials, url = url.split('@', 1)
            if ':' in credentials:
                username, password = credentials.split(':', 1)
            else:
                username = credentials
        return username, password, url
    
    def parse_name(self, fs_name):
        if '#' in fs_name:
            fs_name, fs_name_params = fs_name.split('#', 1)
            return fs_name, fs_name_params
        else:
            return fs_name, None

    def open(self, fs_url, mode='r'):        
        writeable = 'w' in mode or 'a' in mode
        fs, path = self.parse(fs_url, writeable=writeable)
        file_object = fs.open(path, mode)
        return file_object
                    

class Opener(object):
    
    @classmethod
    def get_param(cls, params, name, default=None):        
        try:
            param = params.pop(0)
        except IndexError:
            if default is not None:
                return default
            raise MissingParameterError(error_msg)
        return param


class OSFSOpener(Opener):
    names = ['osfs', 'file']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create):
        from fs.osfs import OSFS 
        username, password, fs_path = registry.parse_credentials(fs_path)                
        
                
        path = _expand_syspath(fs_path)
        if create:
            sys.makedirs(fs_path)
        if os.path.isdir(path):
            osfs = OSFS(path)
            filepath = None
        else:
            path, filepath = pathsplit(path)
            osfs = OSFS(path, create=create)
        return osfs, filepath
        
        
class ZipOpener(Opener):
    names = ['zip', 'zip64']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create):
                                
        append_zip = fs_name_params == 'add'     
                
        zip_fs, zip_path = registry.parse(fs_path)
        if zip_path is None:
            raise OpenerError('File required for zip opener')
        if create:
            open_mode = 'wb'        
            if append_zip:
                open_mode = 'r+b'
        else:
            open_mode = 'rb'
                    
        zip_file = zip_fs.open(zip_path, mode=open_mode)         
                                            
                        
        username, password, fs_path = registry.parse_credentials(fs_path)
        
        from fs.zipfs import ZipFS
        if zip_file is None:            
            zip_file = fs_path
           
        if append_zip:
            mode = 'a'            
        elif create:
            mode = 'w'
        else:
            if writeable:
                mode = 'w'
            else:
                mode = 'a'        
         
        allow_zip_64 = fs_name == 'zip64'                
              
        zipfs = ZipFS(zip_file, mode=mode, allow_zip_64=allow_zip_64)
        return zipfs, None
    
class RPCOpener(Opener):
    names = ['rpc']

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create):
        from fs.rpcfs import RPCFS             
        username, password, fs_path = registry.parse_credentials(fs_path)
        if not fs_path.startswith('http://'):
            fs_path = 'http://' + fs_path
            
        scheme, netloc, path, params, query, fragment = urlparse(fs_path)

        rpcfs = RPCFS('%s://%s' % (scheme, netloc))
        
        if create and path:
            rpcfs.makedir(path, recursive=True, allow_recreate=True)
        
        return rpcfs, path or None

class FTPOpener(Opener):
    names = ['ftp']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create):
        from fs.ftpfs import FTPFS
        username, password, fs_path = registry.parse_credentials(fs_path)
                    
        scheme, netloc, path, params, query, fragment = urlparse(fs_path)
        if not scheme:
            fs_path = 'ftp://' + fs_path
        scheme, netloc, path, params, query, fragment = urlparse(fs_path)
                 
        dirpath, resourcepath = pathsplit(path)        
        url = netloc
                                
        ftpfs = FTPFS(url, user=username or '', passwd=password or '')
        ftpfs.cache_hint(True)
        
        if create and path:
            ftpfs.makedir(path, recursive=True, allow_recreate=True)
        
        if dirpath:
            ftpfs = ftpfs.opendir(dirpath)
                            
        if not resourcepath:
            return ftpfs, None        
        else:
            return ftpfs, resourcepath


class SFTPOpener(Opener):
    names = ['sftp']

    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create):
        username, password, fs_path = registry.parse_credentials(fs_path)        
        
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
            
        #if not username or not password:
        #    raise OpenerError('SFTP requires authentication')
            
        if create:
            sftpfs = SFTPFS(host, root_path='/', **credentials)
            if not sftpfs._transport.is_authenticated():
                sftpfs.close()
                raise OpenerError('SFTP requires authentication')
            sftpfs = sfspfs.makeopendir(fs_path)
            return sftpfs, None
                
        sftpfs = SFTPFS(host, root_path=fs_path, **credentials)
        if not sftpfs._transport.is_authenticated():
            sftpfs.close()
            raise OpenerError('SFTP requires authentication')            
            
        return sftpfs, resourcename
    
    
class MemOpener(Opener):
    names = ['mem', 'ram']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create):
        from fs.memoryfs import MemoryFS
        memfs = MemoryFS()
        if create:
            memfs = memfs.makeopendir(fs_path)
        return memfs, None
    
class DebugOpener(Opener):
    names = ['debug']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create):
        from fs.wrapfs.debugfs import DebugFS
        if fs_path:
            fs, path = registry.parse(fs_path, writeable=writeable, create=create)
            return DebugFS(fs, verbose=False), None     
        if fs_name_params == 'ram':
            from fs.memoryfs import MemoryFS
            return DebugFS(MemoryFS(), identifier=fs_name_params, verbose=False), None
        else:
            from fs.tempfs import TempFS
            return DebugFS(TempFS(), identifier=fs_name_params, verbose=False), None
    
class TempOpener(Opener):
    names = ['temp']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create):
        from fs.tempfs import TempFS        
        return TempFS(identifier=fs_name_params, temp_dir=fs_path), None
    

opener = OpenerRegistry([OSFSOpener,
                         ZipOpener,
                         RPCOpener,
                         FTPOpener,
                         SFTPOpener,
                         MemOpener,
                         DebugOpener,
                         TempOpener,
                         ])
   

def main():
    
    fs, path = opener.parse('sftp://willmcgugan.com')
    print fs, path
    
if __name__ == "__main__":
       
    main()             
    