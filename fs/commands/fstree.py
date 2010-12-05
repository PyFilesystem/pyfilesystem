#!/usr/bin/env python
import sys

from fs.opener import opener
from fs.commands.runner import Command

from fs.utils import print_fs

class FSTree(Command):
        
    def get_optparse(self):
        optparse = super(FSTree, self).get_optparse()        
        optparse.add_option('-d', '--depth', dest='depth', type="int", default=5,
                            help="Maximum depth to display", metavar="DEPTH")        
        return optparse
        
    def do_run(self, options, args):        
        
        if not args:
            args = ['.']
    
        for fs, path, is_dir in self.get_resources(args, single=True):
            if path is not None:
                fs.opendir(path)                
            if not is_dir:
                self.error(u"'%s' is not a dir\n" % path)
                return 1
            print_fs(fs, path or '',
                     file_out=self.output_file,
                     max_levels=options.depth,
                     terminal_colors=self.is_terminal())        
   
def run():
    return FSTree().run()          
    
if __name__ == "__main__":
    sys.exit(run())
   