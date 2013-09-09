#!/usr/bin/env python

import sys

from fs.opener import opener
from fs.commands.runner import Command
from fs.utils import print_fs
import errno


class FSServe(Command):

    usage = """fsserve [OPTION]... [PATH]
Serves the contents of PATH with one of a number of methods"""

    def get_optparse(self):
        optparse = super(FSServe, self).get_optparse()
        optparse.add_option('-t', '--type', dest='type', type="string", default="http",
                            help="Server type to create (http, rpc, sftp)", metavar="TYPE")
        optparse.add_option('-a', '--addr', dest='addr', type="string", default="127.0.0.1",
                            help="Server address", metavar="ADDR")
        optparse.add_option('-p', '--port', dest='port', type="int",
                            help="Port number", metavar="")
        return optparse

    def do_run(self, options, args):

        try:
            fs_url = args[0]
        except IndexError:
            fs_url = './'

        fs, path = self.open_fs(fs_url)

        if fs.isdir(path):
            fs = fs.opendir(path)
            path = '/'

        self.output("Opened %s\n" % fs, verbose=True)

        port = options.port

        try:

            if options.type == 'http':
                from fs.expose.http import serve_fs
                if port is None:
                    port = 80
                self.output("Starting http server on %s:%i\n" % (options.addr, port), verbose=True)
                serve_fs(fs, options.addr, port)

            elif options.type == 'rpc':
                from fs.expose.xmlrpc import RPCFSServer
                if port is None:
                    port = 80
                s = RPCFSServer(fs, (options.addr, port))
                self.output("Starting rpc server on %s:%i\n" % (options.addr, port), verbose=True)
                s.serve_forever()

            elif options.type == 'ftp':
                from fs.expose.ftp import serve_fs
                if port is None:
                    port = 21
                self.output("Starting ftp server on %s:%i\n" % (options.addr, port), verbose=True)
                serve_fs(fs, options.addr, port)

            elif options.type == 'sftp':
                from fs.expose.sftp import BaseSFTPServer
                import logging
                log = logging.getLogger('paramiko')
                if options.debug:
                    log.setLevel(logging.DEBUG)
                elif options.verbose:
                    log.setLevel(logging.INFO)
                ch = logging.StreamHandler()
                ch.setLevel(logging.DEBUG)
                log.addHandler(ch)

                if port is None:
                    port = 22
                server = BaseSFTPServer((options.addr, port), fs)
                try:
                    self.output("Starting sftp server on %s:%i\n" % (options.addr, port), verbose=True)
                    server.serve_forever()
                except Exception, e:
                    pass
                finally:
                    server.server_close()

            else:
                self.error("Server type '%s' not recognised\n" % options.type)

        except IOError, e:
            if e.errno == errno.EACCES:
                self.error('Permission denied\n')
                return 1
            else:
                self.error(str(e) + '\n')
                return 1

def run():
    return FSServe().run()

if __name__ == "__main__":
    sys.exit(run())
