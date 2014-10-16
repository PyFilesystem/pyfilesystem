import warnings
warnings.filterwarnings("ignore")

from fs.opener import opener, OpenerError, Opener
from fs.errors import FSError
from fs.path import splitext, pathsplit, isdotfile, iswildcard

import re
import sys
import platform
import six
from optparse import OptionParser
from collections import defaultdict


if platform.system() == 'Windows':
    def getTerminalSize():
        try:
            ## {{{ http://code.activestate.com/recipes/440694/ (r3)
            from ctypes import windll, create_string_buffer

            # stdin handle is -10
            # stdout handle is -11
            # stderr handle is -12

            h = windll.kernel32.GetStdHandle(-12)
            csbi = create_string_buffer(22)
            res = windll.kernel32.GetConsoleScreenBufferInfo(h, csbi)

            if res:
                import struct
                (bufx, bufy, curx, cury, wattr,
                 left, top, right, bottom, maxx, maxy) = struct.unpack("hhhhHhhhhhh", csbi.raw)
                sizex = right - left + 1
                sizey = bottom - top + 1
            else:
                sizex, sizey = 80, 25  # can't determine actual size - return default values
            return sizex, sizey
        except:
            return 80, 25

else:
    def getTerminalSize():
        def ioctl_GWINSZ(fd):
            try:
                import fcntl, termios, struct, os
                cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
            except:
                return None
            return cr
        cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
        if not cr:
            import os
            try:
                fd = os.open(os.ctermid(), os.O_RDONLY)
                cr = ioctl_GWINSZ(fd)
                os.close(fd)
            except:
                pass
        if cr:
            return int(cr[1]), int(cr[0])
        try:
            h, w = os.popen("stty size", "r").read().split()
            return int(w), int(h)
        except:
            pass
        return 80, 25


def _unicode(text):
    if not isinstance(text, unicode):
        return text.decode('ascii', 'replace')
    return text


class Command(object):

    usage = ''
    version = ''

    def __init__(self, usage='', version=''):
        if six.PY3:
            self.output_file = sys.stdout.buffer
            self.error_file = sys.stderr.buffer
        else:
            self.output_file = sys.stdout
            self.error_file = sys.stderr
        self.encoding = getattr(self.output_file, 'encoding', 'utf-8') or 'utf-8'
        self.verbosity_level = 0
        self.terminal_colors = not sys.platform.startswith('win') and self.is_terminal()
        if self.is_terminal():
            w, _h = getTerminalSize()
            self.terminal_width = w
        else:
            self.terminal_width = 80
        self.name = self.__class__.__name__.lower()

    def is_terminal(self):
        try:
            return self.output_file.isatty()
        except AttributeError:
            return False

    def wrap_dirname(self, dirname):
        if not self.terminal_colors:
            return dirname
        return '\x1b[1;34m%s\x1b[0m' % dirname

    def wrap_error(self, msg):
        if not self.terminal_colors:
            return msg
        return '\x1b[31m%s\x1b[0m' % msg

    def wrap_filename(self, fname):
        fname = _unicode(fname)
        if not self.terminal_colors:
            return fname
        if '://' in fname:
            return fname
#        if '.' in fname:
#            name, ext = splitext(fname)
#            fname = u'%s\x1b[36m%s\x1b[0m' % (name, ext)
        if isdotfile(fname):
            fname = '\x1b[33m%s\x1b[0m' % fname
        return fname

    def wrap_faded(self, text):
        text = _unicode(text)
        if not self.terminal_colors:
            return text
        return u'\x1b[2m%s\x1b[0m' % text

    def wrap_link(self, text):
        if not self.terminal_colors:
            return text
        return u'\x1b[1;33m%s\x1b[0m' % text

    def wrap_strong(self, text):
        if not self.terminal_colors:
            return text
        return u'\x1b[1m%s\x1b[0m' % text

    def wrap_table_header(self, name):
        if not self.terminal_colors:
            return name
        return '\x1b[1;32m%s\x1b[0m' % name

    def highlight_fsurls(self, text):
        if not self.terminal_colors:
            return text
        re_fs = r'(\S*?://\S*)'

        def repl(matchobj):
            fs_url = matchobj.group(0)
            return self.wrap_link(fs_url)
        return re.sub(re_fs, repl, text)

    def open_fs(self, fs_url, writeable=False, create_dir=False):
        fs, path = opener.parse(fs_url, writeable=writeable, create_dir=create_dir)
        fs.cache_hint(True)
        return fs, path

    def expand_wildcard(self, fs, path):
        if path is None:
            return [], []
        pathname, resourcename = pathsplit(path)
        if iswildcard(resourcename):
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
            if path and iswildcard(path):
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

    def output(self, msgs, verbose=False):
        if verbose and not self.options.verbose:
            return
        if isinstance(msgs, basestring):
            msgs = (msgs,)
        for msg in msgs:
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
        for l in lines:
            self.output_file.write(l)
        #self.output(''.join(lines))

    def error(self, *msgs):
        for msg in msgs:
            self.error_file.write("{}: {}".format(self.name, msg).encode(self.encoding))

    def get_optparse(self):
        optparse = OptionParser(usage=self.usage, version=self.version)
        optparse.add_option('--debug', dest='debug', action="store_true", default=False,
                            help="Show debug information", metavar="DEBUG")
        optparse.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
                            help="make output verbose", metavar="VERBOSE")
        optparse.add_option('--listopeners', dest='listopeners', action="store_true", default=False,
                            help="list all FS openers", metavar="LISTOPENERS")
        optparse.add_option('--fs', dest='fs', action='append', type="string",
                            help="import an FS opener e.g --fs foo.bar.MyOpener", metavar="OPENER")
        return optparse

    def list_openers(self):

        opener_table = []

        for fs_opener in opener.openers.itervalues():
            names = fs_opener.names
            desc = getattr(fs_opener, 'desc', '')
            opener_table.append((names, desc))

        opener_table.sort(key=lambda r: r[0])

        def wrap_line(text):

            lines = text.split('\n')
            for line in lines:
                words = []
                line_len = 0
                for word in line.split():
                    if word == '*':
                        word = ' *'
                    if line_len + len(word) > self.terminal_width:
                        self.output((self.highlight_fsurls(' '.join(words)), '\n'))
                        del words[:]
                        line_len = 0
                    words.append(word)
                    line_len += len(word) + 1
                if words:
                    self.output(self.highlight_fsurls(' '.join(words)))
                self.output('\n')

        for names, desc in opener_table:
            self.output(('-' * self.terminal_width, '\n'))
            proto = ', '.join([n + '://' for n in names])
            self.output((self.wrap_dirname('[%s]' % proto), '\n\n'))
            if not desc.strip():
                desc = "No information available"
            wrap_line(desc)
            self.output('\n')

    def run(self):
        parser = self.get_optparse()
        options, args = parser.parse_args()
        self.options = options

        if options.listopeners:
            self.list_openers()
            return 0

        ilocals = {}
        if options.fs:
            for import_opener in options.fs:
                module_name, opener_class = import_opener.rsplit('.', 1)
                try:
                    opener_module = __import__(module_name, globals(), ilocals, [opener_class], -1)
                except ImportError:
                    self.error("Unable to import opener %s\n" % import_opener)
                    return 0

                new_opener = getattr(opener_module, opener_class)

                try:
                    if not issubclass(new_opener, Opener):
                        self.error('%s is not an fs.opener.Opener\n' % import_opener)
                        return 0
                except TypeError:
                    self.error('%s is not an opener class\n' % import_opener)
                    return 0

                if options.verbose:
                    self.output('Imported opener %s\n' % import_opener)

                opener.add(new_opener)

        if not six.PY3:
            args = [unicode(arg, sys.getfilesystemencoding()) for arg in args]
        self.verbose = options.verbose
        try:
            return self.do_run(options, args) or 0
        except FSError, e:
            self.error(self.wrap_error(unicode(e)) + '\n')
            if options.debug:
                raise
            return 1
        except KeyboardInterrupt:
            if self.is_terminal():
                self.output("\n")
            return 0
        except SystemExit:
            return 0
        except Exception, e:
            self.error(self.wrap_error('Error - %s\n' % unicode(e)))
            if options.debug:
                raise
            return 1


if __name__ == "__main__":
    command = Command()
    sys.exit(command.run())