#!/usr/bin/env python
from fs.commands.runner import Command
import sys

class FSMkdir(Command):
    
    usage = """fsmkdir [PATH]
Make a directory"""

    version = "1.0"
    
    def do_run(self, options, args):
                
        for fs_url in args:                    
            self.open_fs(fs_url, create_dir=True)                        
    
def run():
    return FSMkdir().run()
    
if __name__ == "__main__":
    sys.exit(run())
        