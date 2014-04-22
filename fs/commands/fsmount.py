#!/usr/bin/env python
from fs.commands.runner import Command
import sys
import platform
import os
import os.path


platform = platform.system()


class FSMount(Command):

    if platform == "Windows":
        usage = """fsmount [OPTIONS]... [FS] [DRIVE LETTER]
or fsmount -u [DRIVER LETTER]
Mounts a filesystem on a drive letter"""
    else:
        usage = """fsmount [OPTIONS]... [FS] [SYSTEM PATH]
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

        windows = platform == "Windows"

        if options.unmount:

            if windows:

                try:
                    mount_path = args[0][:1]
                except IndexError:
                    self.error('Driver letter required\n')
                    return 1

                from fs.expose import dokan
                mount_path = mount_path[:1].upper()
                self.output('unmounting %s:...\n' % mount_path, True)
                dokan.unmount(mount_path)
                return

            else:
                try:
                    mount_path = args[0]
                except IndexError:
                    self.error(self.usage + '\n')
                    return 1

                from fs.expose import fuse
                self.output('unmounting %s...\n' % mount_path, True)
                fuse.unmount(mount_path)
                return

        try:
            fs_url = args[0]
        except IndexError:
            self.error(self.usage + '\n')
            return 1

        try:
            mount_path = args[1]
        except IndexError:
            if windows:
                mount_path = mount_path[:1].upper()
                self.error(self.usage + '\n')
            else:
                self.error(self.usage + '\n')
            return 1

        fs, path = self.open_fs(fs_url, create_dir=True)
        if path:
            if not fs.isdir(path):
                self.error('%s is not a directory on %s' % (fs_url, fs))
                return 1
            fs = fs.opendir(path)
            path = '/'
        if not options.nocache:
            fs.cache_hint(True)

        if windows:
            from fs.expose import dokan

            if len(mount_path) > 1:
                self.error('Driver letter should be one character')
                return 1

            self.output("Mounting %s on %s:\n" % (fs, mount_path), True)
            flags = dokan.DOKAN_OPTION_REMOVABLE
            if options.debug:
                flags |= dokan.DOKAN_OPTION_DEBUG | dokan.DOKAN_OPTION_STDERR

            mp = dokan.mount(fs,
                             mount_path,
                             numthreads=5,
                             foreground=options.foreground,
                             flags=flags,
                             volname=str(fs))

        else:
            if not os.path.exists(mount_path):
                try:
                    os.makedirs(mount_path)
                except:
                    pass

            from fs.expose import fuse
            self.output("Mounting %s on %s\n" % (fs, mount_path), True)

            if options.foreground:
                fuse_process = fuse.mount(fs,
                                          mount_path,
                                          foreground=True)
            else:
                if not os.fork():
                    mp = fuse.mount(fs,
                                    mount_path,
                                    foreground=True)
                else:
                    fs.close = lambda: None

def run():
    return FSMount().run()

if __name__ == "__main__":
    sys.exit(run())
