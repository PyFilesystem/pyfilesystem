#!/usr/bin/env python
import sys

from fs.opener import opener
from fs.commands.runner import Command
from fs.utils import print_fs


class FSServe(Command):
    
    usage = """fsserve [OPTION]... [PATH]
Serves the contents of PATH with one of a number of methods"""
    
    def get_optparse(self):
        optparse = super(FSServe, self).get_optparse()                
        optparse.add_option('-t', '--type', dest='type', type="string", default="http",
                            help="Server type to create (http, rpc, sftp)", metavar="TYPE")
        optparse.add_option('-a', '--addr', dest='addr', type="string", default="",
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
        
        if options.verbose:
            print "Serving \"%s\" in %s" % (path, fs)
                    
        port = options.port
            
        try:
                                
            if options.type == 'http':            
                from fs.expose.http import serve_fs            
                if port is None:
                    port = 80                            
                serve_fs(fs, options.addr, port)            
                    
            elif options.type == 'rpc':            
                from fs.expose.xmlrpc import RPCFSServer
                if port is None:
                    port = 80
                s = RPCFSServer(fs, (options.addr, port))
                s.serve_forever()
            
            elif options.type == 'sftp':            
                from fs.expose.sftp import BaseSFTPServer
                if port is None:
                    port = 22
                server = BaseSFTPServer((options.addr, port), fs)
                try:
                    server.serve_forever()
                except Exception, e:
                    pass
                finally:
                    server.server_close()
            
            else:
                self.error("Server type '%s' not recognised\n" % options.type)
            
        except IOError, e:
            if e.errno == 13:
                self.error('Permission denied\n')
                return 1
            else:                
                self.error(e.strerror + '\n')
                return 1            
            
def run():                               
    return FSServe().run()        
    
if __name__ == "__main__":
    sys.exit(run())