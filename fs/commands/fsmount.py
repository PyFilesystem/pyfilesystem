#!/usr/bin/env python

from fs.opener import opener
from fs.commands.runner import Command
import sys
import platform
import os
import os.path
import time

class FSMount(Command):
    
    usage = """fsmount [SYSTEM PATH] [FS]
or fsmount -u [SYSTEM PATH]
Mounts a file system on a system path"""

    version = "1.0"
    
    def get_optparse(self):
        optparse = super(FSMount, self).get_optparse()        
        optparse.add_option('-f', '--foreground', dest='foreground', action="store_true", default=False,
                            help="run the mount process in the foreground", metavar="FOREGROUND")
        optparse.add_option('-u', '--unmount', dest='unmount', action="store_true", default=False,
                            help="unmount path", metavar="UNMOUNT")
        optparse.add_option('-n', '--nocache', dest='nocache', action="store_true", default=False,
                            help="do not cache network filesystems", metavar="NOCACHE")
        
        return optparse
    
    
    def do_run(self, options, args):
        
        if options.unmount:
            try:                
                mount_path = args[0]
            except IndexError:
                self.error('Mount path required\n')
                return 1
            from fs.expose import fuse
            fuse.unmount(mount_path)
            return
             
        try:                
            mount_path = args[0]
        except IndexError:
            self.error('Mount path required\n')
            return 1
        try:
            fs_url = args[1]
        except IndexError:
            self.error('FS required\n')
            return 1                    
             
        if platform.system() == 'Windows':
            pass
        else:
            fs, path = self.open_fs(fs_url, create_dir=True)
            if path:
                if not fs.isdir(path):
                    self.error('%s is not a directory on %s' % (fs_url. fs))
                    return 1
                fs = fs.opendir(path)
                path = '/'
            if not options.nocache:
                fs.cache_hint(True)
            if not os.path.exists(mount_path):
               os.makedirs(mount_path)
            from fs.expose import fuse
            if options.foreground:                                            
                fuse_process = fuse.mount(fs,
                                          mount_path,
                                          foreground=True)                                                    
            else:
                if not os.fork():                
                    mp = fuse.mount(fs,
                                    mount_path,
                                    foreground=True)
                

    
def run():
    return FSMount().run()
    
if __name__ == "__main__":
    sys.exit(run())
        