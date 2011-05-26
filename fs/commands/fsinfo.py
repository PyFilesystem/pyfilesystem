#!/usr/bin/env python
from fs.commands.runner import Command
import sys
from datetime import datetime

class FSInfo(Command):
     
    usage = """fsinfo [OPTION]... [PATH]
Display information regarding an FS resource"""
     
    def get_optparse(self):
        optparse = super(FSInfo, self).get_optparse()
        optparse.add_option('-k', '--key', dest='keys', action='append', default=[],
                            help='display KEYS only')
        optparse.add_option('-s', '--simple', dest='simple', action='store_true', default=False,
                            help='info displayed in simple format (no table)')
        optparse.add_option('-o', '--omit', dest='omit', action='store_true', default=False,
                            help='omit path name from output')
        optparse.add_option('-d', '--dirsonly', dest='dirsonly', action="store_true", default=False,
                            help="list directories only", metavar="DIRSONLY")
        optparse.add_option('-f', '--filesonly', dest='filesonly', action="store_true", default=False,
                            help="list files only", metavar="FILESONLY")
        return optparse   
        
        
    def do_run(self, options, args):
        
        def wrap_value(val):            
            if val.rstrip() == '\0':
                return self.wrap_error('... missing ...')
            return val

        def make_printable(text):
            if not isinstance(text, basestring):
                try:
                    text = str(text)
                except:
                    try:
                        text = unicode(text)
                    except:
                        text = repr(text)
            return text
                
                
        keys = options.keys or None
        for fs, path, is_dir in self.get_resources(args,
                                                   files_only=options.filesonly,
                                                   dirs_only=options.dirsonly):                        
            if not options.omit:
                if options.simple:           
                    file_line = u'%s\n' % self.wrap_filename(path)
                else:
                    file_line = u'[%s] %s\n' % (self.wrap_filename(path), self.wrap_faded(fs.desc(path)))
                self.output(file_line)            
            info = fs.getinfo(path)
                                            
            for k, v in info.items():
                if k.startswith('_'):
                    del info[k]
                elif not isinstance(v, (basestring, int, long, float, bool, datetime)):
                    del info[k]                
                        
            if keys:            
                table = [(k, make_printable(info.get(k, '\0'))) for k in keys]
            else:
                keys = sorted(info.keys())
                table = [(k, make_printable(info[k])) for k in sorted(info.keys())]
                                 
            if options.simple:
                for row in table:
                    self.output(row[-1] + '\n')
            else:                                        
                self.output_table(table, {0:self.wrap_table_header, 1:wrap_value})            

        
def run():
    return FSInfo().run()         
    
if __name__ == "__main__":
    sys.exit(run())    
