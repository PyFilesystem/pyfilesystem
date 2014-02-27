#!/usr/bin/env python

from fs.errors import FSError
from fs.opener import opener
from fs.path import pathsplit, abspath, isdotfile, iswildcard
from fs.commands.runner import Command
from collections import defaultdict
import sys

class FSls(Command):

    usage = """fsls [OPTIONS]... [PATH]
List contents of [PATH]"""


    def get_optparse(self):
        optparse = super(FSls, self).get_optparse()
        optparse.add_option('-u', '--full', dest='fullpath', action="store_true", default=False,
                            help="output full path", metavar="FULL")
        optparse.add_option('-s', '--syspath', dest='syspath', action="store_true", default=False,
                            help="output system path (if one exists)", metavar="SYSPATH")
        optparse.add_option('-r', '--url', dest='url', action="store_true", default=False,
                            help="output URL in place of path (if one exists)", metavar="URL")
        optparse.add_option('-d', '--dirsonly', dest='dirsonly', action="store_true", default=False,
                            help="list directories only", metavar="DIRSONLY")
        optparse.add_option('-f', '--filesonly', dest='filesonly', action="store_true", default=False,
                            help="list files only", metavar="FILESONLY")
        optparse.add_option('-l', '--long', dest='long', action="store_true", default=False,
                            help="use a long listing format", metavar="LONG")
        optparse.add_option('-a', '--all', dest='all', action='store_true', default=False,
                            help="do not hide dot files")

        return optparse


    def do_run(self, options, args):
        output = self.output

        if not args:
            args = [u'.']

        dir_paths = []
        file_paths = []
        fs_used = set()
        for fs_url in args:
            fs, path = self.open_fs(fs_url)
            fs_used.add(fs)
            path = path or '.'
            wildcard = None

            if iswildcard(path):
                path, wildcard = pathsplit(path)

            if path != '.' and fs.isfile(path):
                if not options.dirsonly:
                    file_paths.append(path)
            else:
                if not options.filesonly:
                    dir_paths += fs.listdir(path,
                                            wildcard=wildcard,
                                            full=options.fullpath or options.url,
                                            dirs_only=True)

                if not options.dirsonly:
                    file_paths += fs.listdir(path,
                                             wildcard=wildcard,
                                             full=options.fullpath or options.url,
                                             files_only=True)

        for fs in fs_used:
            try:
                fs.close()
            except FSError:
                pass

        if options.syspath:
            # Path without a syspath, just won't be displayed
            dir_paths = filter(None, [fs.getsyspath(path, allow_none=True) for path in dir_paths])
            file_paths = filter(None, [fs.getsyspath(path, allow_none=True) for path in file_paths])

        if options.url:
            # Path without a syspath, just won't be displayed
            dir_paths = filter(None, [fs.getpathurl(path, allow_none=True) for path in dir_paths])
            file_paths = filter(None, [fs.getpathurl(path, allow_none=True) for path in file_paths])

        dirs = frozenset(dir_paths)
        paths = sorted(dir_paths + file_paths, key=lambda p: p.lower())

        if not options.all:
            paths = [path for path in paths if not isdotfile(path)]

        if not paths:
            return

        def columnize(paths, num_columns):

            col_height = (len(paths) + num_columns - 1) / num_columns
            columns = [[] for _ in xrange(num_columns)]
            col_no = 0
            col_pos = 0
            for path in paths:
                columns[col_no].append(path)
                col_pos += 1
                if col_pos >= col_height:
                    col_no += 1
                    col_pos = 0

            padded_columns = []

            wrap_filename = self.wrap_filename
            wrap_dirname = self.wrap_dirname

            def wrap(path):
                if path in dirs:
                    return wrap_dirname(path.ljust(max_width))
                else:
                    return wrap_filename(path.ljust(max_width))

            for column in columns:
                if column:
                    max_width = max([len(path) for path in column])
                else:
                    max_width = 1
                max_width = min(max_width, terminal_width)
                padded_columns.append([wrap(path) for path in column])

            return padded_columns

        def condense_columns(columns):
            max_column_height = max([len(col) for col in columns])
            lines = [[] for _ in xrange(max_column_height)]
            for column in columns:
                for line, path in zip(lines, column):
                    line.append(path)
            return '\n'.join(u'  '.join(line) for line in lines)

        if options.long:
            for path in paths:
                if path in dirs:
                    output((self.wrap_dirname(path), '\n'))
                else:
                    output((self.wrap_filename(path), '\n'))

        else:
            terminal_width = self.terminal_width
            path_widths = [len(path) for path in paths]
            smallest_paths = min(path_widths)
            num_paths = len(paths)

            num_cols = min(terminal_width // (smallest_paths + 2), num_paths)
            while num_cols:
                col_height = (num_paths + num_cols - 1) // num_cols
                line_width = 0
                for col_no in xrange(num_cols):
                    try:
                        col_width = max(path_widths[col_no * col_height: (col_no + 1) * col_height])
                    except ValueError:
                        continue
                    line_width += col_width
                    if line_width > terminal_width:
                        break
                    line_width += 2
                else:
                    if line_width - 1 <= terminal_width:
                        break
                num_cols -= 1
            num_cols = max(1, num_cols)
            columns = columnize(paths, num_cols)
            output((condense_columns(columns), '\n'))

def run():
    return FSls().run()

if __name__ == "__main__":
    sys.exit(run())
