import sys
from fs.osfs import OSFS
from fs.path import pathsplit
import os.path
import re

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
(?:\[(.*?)\])*

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
        assert match is not None, "broken re?"
        return match.groups()        
    
    def get_opener(self, name):
        if name not in self.registry:
            raise NoOpenerError("No opener for [%s]" % name)
        index = self.registry[name]
        return self.openers[index]        
    
    def add(self, opener):
        index = len(self.openers)
        self.openers[index] = opener
        for name in opener.names:
            self.registry[name] = index
    
    def parse(self, fs_url, default_fs_name=None, writeable=False, create=False):
                        
        fs_name, paren_url, fs_url, path = self.split_segments(fs_url)
        
        if fs_name is None and path is None:
            fs_url, path = pathsplit(fs_url)
            if not fs_url:
                fs_url = '/'
        
        fs_name = fs_name or self.default_opener        
        fs_url = fs_url or paren_url                  
        
        if fs_name is None:
            fs_name = fs_default_name
        
        fs_name,  fs_name_params = self.parse_name(fs_name)        
        opener = self.get_opener(fs_name)
                   
        fs = opener.get_fs(self, fs_name, fs_name_params, fs_url, writeable, create)
        
        if path:
            pathname, resourcename = pathsplit(path)
            if pathname:
                fs = fs.opendir(pathname)
                path = resourcename
        
        return fs, path
    
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
        username, password, fs_path = registry.parse_credentials(fs_path)       
        from fs.osfs import OSFS
        osfs = OSFS(fs_path, create=create)
        return osfs
        
        
class ZipOpener(Opener):
    names = ['zip', 'zip64']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create):
                
        
        create_zip = fs_name_params == 'new'        
        
        zip_file = None
        if fs_path.startswith('['):
            container_fs, container_path = registry.parse(fs_path)
            if not container_path:
                raise OpenerError("Not a file")
            container_mode = 'r+b'
            if create_zip:
                container_mode = 'w+'
            zip_file = container_fs.open(container_path, mode=container_mode)                            
                        
        username, password, fs_path = registry.parse_credentials(fs_path)
        
        from fs.zipfs import ZipFS
        if zip_file is None:            
            zip_file = fs_path
            
        if create_zip:
            mode = 'w'
        else:
            if writeable:
                mode = 'a'
            else:
                mode = 'r'        
                
        if fs_name == 'zip64':
            allow_zip_64 = True
        else:
            allow_zip_64 = False
        zipfs = ZipFS(zip_file, mode=mode, allow_zip_64=allow_zip_64)
        return zipfs
    

class FTPOpener(Opener):
    names = ['ftp']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path, writeable, create):
        from fs.ftpfs import FTPFS
        username, password, fs_path = registry.parse_credentials(fs_path)
        
        if '/' in fs_path:
            url, root_path = fs_path.split('/', 1)
        else:
            url = fs_path
            root_path = ''
                                                                
        ftpfs = FTPFS(url, user=username or '', passwd=password or '')
        ftpfs.cache_hint(True)
        
        if root_path not in ('', '/'):
            if not ftpfs.isdir(root_path):
                raise OpenerError("'%s' is not a directory on the server" % root_path)
            return ftpfs.opendir(root_path)
        
        return ftpfs


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
                
        sftpfs = SFTPFS(host, root_path=fs_path, **credentials)
        return sftpfs
    
    
class MemOpener(Opener):
    names = ['mem', 'ram']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create):
        from fs.memoryfs import MemoryFS
        return MemoryFS()
    
class DebugOpener(Opener):
    names = ['debug']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create):
        from fs.wrapfs.debugfs import DebugFS
        if fs_path:
            fs, path = registry.parse(fs_path)
            return DebugFS(fs, verbose=False)     
        if fs_name_params == 'ram':
            from fs.memoryfs import MemoryFS
            return DebugFS(MemoryFS(), identifier=fs_name_params, verbose=False)
        else:
            from fs.tempfs import TempFS
            return DebugFS(TempFS(), identifier=fs_name_params, verbose=False)
    
class TempOpener(Opener):
    names = ['temp']
    
    @classmethod
    def get_fs(cls, registry, fs_name, fs_name_params, fs_path,  writeable, create):
        from fs.tempfs import TempFS        
        return TempFS(identifier=fs_name_params, temp_dir=fs_path)
    

opener = OpenerRegistry([OSFSOpener,
                         ZipOpener,
                         FTPOpener,
                         SFTPOpener,
                         MemOpener,
                         DebugOpener,
                         TempOpener,
                         ])
   

def main():
    
    #fs, path = opener.parse('*.py')
    #fs, path = opener.parse('[osfs]/home/will/+projects/pyfilesystem')
    
    #fs, path = opener.parse('~/t.zip')
    fs, path = opener.parse('[sftp]will:password')
    #fs, path = opener.parse('[zip]([osfs]~/+test.zip)+a.txt')
    
    print fs, path
    
    #fs, path = opener.parse('[zip#[sftp]root:hamster5921@willmcgugan.com+/home/www/willmcgugan.com/files/langtonants.zip]+/langtonants')
    #fs, path = opener.parse('[sftp]root:hamster5921@willmcgugan.com+chesscommander+')
    #print fs
    #print path    
    #print fs.opendir(path).listdir()
    
if __name__ == "__main__":
       
    main()             
    #fs, path = opener.parse('[ftp]ftp.mozilla.org+pub')
    #print fs.listdir(path)
                
    #print registry.parse("/home/will/Pictures/darkpie.png")    
    #print registry.parse("[file]/home/will/Pictures/+darkpie.png")
    #print registry.parse("/home/will/Pictures/+darkpie.png")
    #print registry.parse("/home/will/Pictures/")
    #print registry.parse("[zip]/home/will/Pictures/Pictures.zip+DSC00717.JPG")
    #print registry.open("[zip]/home/will/Pictures/t.zip+t.txt").read()
    #print registry.getcontents("[zip]/home/will/Pictures/t.zip+t.txt")
    #print registry.parse("ftp:ftp.site.org+/some/site")