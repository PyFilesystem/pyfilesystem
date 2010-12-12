#!/usr/bin/env python

from fs.errors import ResourceNotFoundError
from fs.opener import opener
from fs.commands.runner import Command
import sys

class FSrm(Command):
    
    usage = """fsrm [OPTION]... [PATH]
Remove a file or directory at PATH"""
    
    def get_optparse(self):
        optparse = super(FSrm, self).get_optparse()
        optparse.add_option('-f', '--force', dest='force', action='store_true', default=False,
                            help='ignore non-existent files, never prompt')
        optparse.add_option('-i', '--interactive', dest='interactive', action='store_true', default=False,
                            help='prompt before removing')
        optparse.add_option('-r', '--recursive', dest='recursive', action='store_true', default=False,
                            help='remove directories and their contents recursively')
        return optparse
    
    def do_run(self, options, args):
                        
        interactive = options.interactive
        verbose = options.verbose
        
        for fs, path, is_dir in self.get_resources(args):                              
            if interactive:
                if is_dir:
                    msg = "remove directory '%s'?" % path
                else:
                    msg = "remove file '%s'?" % path
                if not self.ask(msg) in 'yY':
                    continue
            try:
                if is_dir:
                    fs.removedir(path, force=options.recursive)
                else:
                    fs.remove(path)
            except ResourceNotFoundError:
                if not options.force:
                    raise
            else:
                if verbose:
                    self.output("removed '%s'\n" % path)
            
        
def run():         
    return FSrm().run()
    
if __name__ == "__main__":
    sys.exit(run())    
    