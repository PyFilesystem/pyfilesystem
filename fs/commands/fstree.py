#!/usr/bin/env python
import sys

from fs.opener import opener
from fs.commands.runner import Command

from fs.utils import print_fs

class FSTree(Command):
    
    usage = """fstree [OPTION]... [PATH]
Recursively display the contents of PATH in an ascii tree"""
    
    def get_optparse(self):
        optparse = super(FSTree, self).get_optparse()        
        optparse.add_option('-l', '--level', dest='depth', type="int", default=5,
                            help="Descend only LEVEL directories deep (-1 for infinite)", metavar="LEVEL")
        optparse.add_option('-g', '--gui', dest='gui', action='store_true', default=False,
                            help="browse the tree with a gui")
        optparse.add_option('-a', '--all', dest='all', action='store_true', default=False,
                            help="do not hide dot files")        
        optparse.add_option('--dirsfirst', dest='dirsfirst', action='store_true', default=False,
                            help="List directories before files")
        optparse.add_option('-P', dest="pattern", default=None,
                            help="Only list files that match the given pattern")
        optparse.add_option('-d', dest="dirsonly", default=False, action='store_true',
                            help="List directories only")
        return optparse
        
    def do_run(self, options, args):        
        
        if not args:
            args = ['.']
    
        for fs, path, is_dir in self.get_resources(args, single=True):                            
            if not is_dir:
                self.error(u"'%s' is not a dir\n" % path)
                return 1
            fs.cache_hint(True)
            if options.gui:
                from fs.browsewin import browse
                if path:
                    fs = fs.opendir(path)
                browse(fs, hide_dotfiles=not options.all)
            else:
                if options.depth < 0:
                    max_levels = None
                else:
                    max_levels = options.depth
                self.output(self.wrap_dirname(args[0] + '\n'))
                dircount, filecount = print_fs(fs, path or '',
                                               file_out=self.output_file,
                                               max_levels=max_levels,
                                               terminal_colors=self.terminal_colors,
                                               hide_dotfiles=not options.all,
                                               dirs_first=options.dirsfirst,
                                               files_wildcard=options.pattern,
                                               dirs_only=options.dirsonly)
                self.output('\n')
                def pluralize(one, many, count):
                    if count == 1:
                        return '%i %s' % (count, one)
                    else:
                        return '%i %s' % (count, many)
                
                self.output("%s, %s\n" % (pluralize('directory', 'directories', dircount),
                                  pluralize('file', 'files', filecount)))
   
def run():
    return FSTree().run()          
    
if __name__ == "__main__":
    sys.exit(run())
   
