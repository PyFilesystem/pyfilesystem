import sys
from optparse import OptionParser
from fs.opener import opener, OpenerError
from fs.errors import FSError
from fs.path import splitext, pathsplit, isdotfile
import platform
from collections import defaultdict


if platform.system() == 'Linux' :
    def getTerminalSize():
        def ioctl_GWINSZ(fd):
            try:
                import fcntl, termios, struct, os
                cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
            '1234'))
            except:
                return None
            return cr
        cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
        if not cr:
            try:
                fd = os.open(os.ctermid(), os.O_RDONLY)
                cr = ioctl_GWINSZ(fd)
                os.close(fd)
            except:
                pass
        if not cr:
            try:
                cr = (env['LINES'], env['COLUMNS'])
            except:
                cr = (25, 80)
        return int(cr[1]), int(cr[0])
else:
    def getTerminalSize():
        return 80, 50

def _unicode(text):
    if not isinstance(text, unicode):
        return text.decode('ascii', 'replace')
    return text

class Command(object):
    
    usage = ''
    version = ''
    
    def __init__(self, usage='', version=''):        
        self.output_file = sys.stdout
        self.error_file = sys.stderr
        self.encoding = getattr(self.output_file, 'encoding', 'utf-8') or 'utf-8'
        self.verbosity_level = 0
        self.terminal_colors = not sys.platform.startswith('win') and self.is_terminal()
        w, h = getTerminalSize()
        self.terminal_width = w
        self.name = self.__class__.__name__.lower()
    
    def is_wildcard(self, path):
        if path is None:
            return False
        return '*' in path or '?' in path
    
    def is_terminal(self):
        try:
            return self.output_file.isatty()
        except AttributeError:
            return False        
        
    def wrap_dirname(self, dirname):
        if not self.terminal_colors:
            return dirname
        return '\x1b[1;32m%s\x1b[0m' % dirname
    
    def wrap_error(self, msg):
        if not self.terminal_colors:
            return msg
        return '\x1b[31m%s\x1b[0m' % msg
    
    def wrap_filename(self, fname):        
        fname = _unicode(fname)
        if not self.terminal_colors:
            return fname            
        if '.' in fname:
            name, ext = splitext(fname)
            fname = u'%s\x1b[36m%s\x1b[0m' % (name, ext)
        if isdotfile(fname):
            fname = u'\x1b[2m%s\x1b[0m' % fname
        return fname
    
    def wrap_faded(self, text):
        text = _unicode(text)
        if not self.terminal_colors:
            return text
        return u'\x1b[2m%s\x1b[0m' % text
    
    def wrap_table_header(self, name):
        if not self.terminal_colors:
            return name
        return '\x1b[1;32m%s\x1b[0m' % name
        
    def open_fs(self, fs_url, writeable=False, create=False):
        try:
            fs, path = opener.parse(fs_url, writeable=writeable, create=create)
        except OpenerError, e:
            self.error(str(e)+'\n')
            sys.exit(1)
        fs.cache_hint(True)        
        return fs, path
    
    def expand_wildcard(self, fs, path):        
        if path is None:
            return [], []
        pathname, resourcename = pathsplit(path)
        if self.is_wildcard(resourcename):
            dir_paths = fs.listdir(pathname,
                                   wildcard=resourcename,
                                   absolute=True,
                                   dirs_only=True)
            
            file_paths = fs.listdir(pathname,
                                    wildcard=resourcename,
                                    absolute=True,
                                    files_only=True)
            return dir_paths, file_paths
            
        else:            
            if fs.isdir(path):                
                #file_paths = fs.listdir(path,
                #                        absolute=True)
                return [path], []
            return [], [path]
        
    def get_resources(self, fs_urls, dirs_only=False, files_only=False, single=False):
        
        fs_paths = [self.open_fs(fs_url) for fs_url in fs_urls]        
        
        resources = []
        
        for fs, path in fs_paths:
            if self.is_wildcard(path):
                if not files_only:
                    dir_paths = fs.listdir(wildcard=path, dirs_only=True)
                    for path in dir_paths:
                        resources.append([fs, path, True])                        
                if not dirs_only:                    
                    file_paths = fs.listdir(wildcard=path, files_only=True)
                    for path in file_paths:
                        resources.append([fs, path, False])                        
            else:
                path = path or '/'
                is_dir = fs.isdir(path)
                resource = [fs, path, is_dir]
                if not files_only and not dirs_only:
                    resources.append(resource)
                elif files_only and not is_dir:
                    resources.append(resource)
                elif dirs_only and is_dir:
                    resources.append(resource)                
                    
            if single:
                break
            
        return resources                
    
    def ask(self, msg):
        return raw_input('%s: %s ' % (self.name, msg))                     
    
    def text_encode(self, text):  

        if not isinstance(text, unicode):
            text = text.decode('ascii', 'replace')                
        text = text.encode(self.encoding, 'replace')
                    
        return text
    
    def output(self, msg, verbose=False):
        if verbose and not self.verbose:
            return        
        self.output_file.write(self.text_encode(msg))
        
    
    def output_table(self, table, col_process=None, verbose=False):
        
        if verbose and not self.verbose:
            return
        
        if col_process is None:
            col_process = {}
        
        max_row_widths = defaultdict(int)
        
        for row in table:
            for col_no, col in enumerate(row):
                max_row_widths[col_no] = max(max_row_widths[col_no], len(col))
        
        lines = []        
        for row in table:
            out_col = []
            for col_no, col in enumerate(row):
                td = col.ljust(max_row_widths[col_no])
                if col_no in col_process:
                     td = col_process[col_no](td)
                out_col.append(td)
            lines.append(self.text_encode('%s\n' % '  '.join(out_col).rstrip()))
        self.output(''.join(lines))        
                                        
    def error(self, msg):
        self.error_file.write('%s: %s' % (self.name, self.text_encode(msg)))        
        
    def get_optparse(self):
        optparse = OptionParser(usage=self.usage, version=self.version)
        optparse.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
                            help="make output verbose", metavar="VERBOSE")        
        return optparse
        
    def run(self):        
        parser = self.get_optparse()
        options, args = parser.parse_args()
        args = [unicode(arg, sys.getfilesystemencoding()) for arg in args]
        self.verbose = options.verbose        
        try:
            return self.do_run(options, args) or 0
        except FSError, e:
            self.error(self.wrap_error(unicode(e)) + '\n')
            return 1        
        except KeyboardInterrupt:
            if self.is_terminal():
                self.output("\n")
            return 0
        except SystemExit:
            return 0
        #except IOError:
        #    return 1
        #except Exception, e:            
        #    self.error(self.wrap_error('Internal Error - %s\n' % unicode(e)))
        #    return 1
        
        
        
if __name__ == "__main__":
    command = Command()
    sys.exit(command.run())