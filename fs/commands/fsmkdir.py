#!/usr/bin/env python

from fs.opener import opener
from fs.commands.runner import Command
import sys

class FSMkdir(Command):
    
    usage = """fsmkdir [PATH]
Make a directory"""

    version = "1.0"
    
    def do_run(self, options, args):
                
        for fs_url in args:            
            fs, path = self.open_fs(fs_url, create=True)                
    
def run():
    return FSMkdir().run()
    
if __name__ == "__main__":
    sys.exit(run())
        