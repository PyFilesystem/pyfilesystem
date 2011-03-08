#!/usr/bin/env python
from fs.commands.runner import Command
import sys

class FSCat(Command):
    
    usage = """fscat [OPTION]... [FILE]...
Concetanate FILE(s)"""

    version = "1.0"
    
    def do_run(self, options, args):
        count = 0            
        for fs, path, is_dir in self.get_resources(args):            
            if is_dir:
                self.error('%s is a directory\n' % path)
                return 1                      
            self.output(fs.getcontents(path))
            count += 1              
        if self.is_terminal() and count:
            self.output('\n')
    
def run():
    return FSCat().run()
    
if __name__ == "__main__":
    sys.exit(run())
        