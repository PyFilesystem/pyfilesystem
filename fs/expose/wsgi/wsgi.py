
import urlparse
import mimetypes

from fs.errors import FSError
from fs.path import basename, pathsplit

from datetime import datetime

try:
    from mako.template import Template
except ImportError:
    print "Requires mako templates http://www.makotemplates.org/"
    raise


class Request(object):
    """Very simple request object"""
    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response        
        self.path = environ.get('PATH_INFO')
        

class WSGIServer(object):
    """Light-weight WSGI server that exposes an FS"""
    
    def __init__(self, serve_fs, indexes=True, dir_template=None, chunk_size=16*1024*1024):
        
        if dir_template is None:
            from dirtemplate import template as dir_template            
        
        self.serve_fs = serve_fs
        self.indexes = indexes        
        self.chunk_size = chunk_size
        
        self.dir_template = Template(dir_template)     
     
    def __call__(self, environ, start_response):
        
        request = Request(environ, start_response)
        
        if not self.serve_fs.exists(request.path):
            return self.serve_404(request)     
        
        if self.serve_fs.isdir(request.path):
            if not self.indexes:
                return self.serve_404(request)
            return self.serve_dir(request)
        else:
            return self.serve_file(request)
    
    
    def serve_file(self, request):    
        """Serve a file, guessing a mime-type"""
        path = request.path                                            
        serving_file = None
        try:            
            serving_file = self.serve_fs.open(path, 'rb')
        except Exception, e:
            if serving_file is not None:
                serving_file.close()
            return self.serve_500(request, str(e))
        
        mime_type = mimetypes.guess_type(basename(path))        
        file_size = self.serve_fs.getsize(path)
        headers = [('Content-Type', mime_type),
                   ('Content-Length', str(file_size))]
        
        def gen_file():
            try:
                while True:
                    data = serving_file.read(self.chunk_size)
                    if not data:
                        break
                    yield data
            finally:
                serving_file.close()
                
        request.start_response('200 OK',
                               headers)
        return gen_file()                
    
    def serve_dir(self, request):
        """Serve an index page"""
        fs = self.serve_fs
        isdir = fs.isdir        
        path = request.path                     
        dirinfo = fs.listdirinfo(path, full=True, absolute=True)        
        entries = []
        
        for p, info in dirinfo:
            entry = {}
            entry['path'] = p
            entry['name'] = basename(p)
            entry['size'] = info.get('size', 'unknown')
            entry['created_time'] = info.get('created_time')                                                        
            if isdir(p):
                entry['type'] = 'dir'
            else:
                entry['type'] = 'file'                
                
            entries.append(entry)
            
        # Put dirs first, and sort by reverse created time order
        no_time = datetime(1970, 1, 1, 1, 0)
        entries.sort(key=lambda k:(k['type'] == 'dir', k.get('created_time') or no_time), reverse=True)
        
        # Turn datetime to text and tweak names
        for entry in entries:
            t = entry.get('created_time')
            if t and hasattr(t, 'ctime'):
                entry['created_time'] = t.ctime()
            if entry['type'] == 'dir':
                entry['name'] += '/'
    
        # Add an up dir link for non-root
        if path not in ('', '/'):
            entries.insert(0, dict(name='../', path='../', type="dir", size='', created_time='..'))
            
        # Render the mako template
        html = self.dir_template.render(**dict(fs=self.serve_fs,
                                               path=path,
                                               dirlist=entries))
        
        request.start_response('200 OK', [('Content-Type', 'text/html'),
                                          ('Content-Length', '%i' % len(html))])
        
        return [html]
        
            
    def serve_404(self, request, msg='Not found'):
        """Serves a Not found page"""
        request.start_response('404 NOT FOUND', [('Content-Type', 'text/html')])
        return [msg]
    
    def serve_500(self, request, msg='Unable to complete request'):
        """Serves an internal server error page"""        
        request.start_response('500 INTERNAL SERVER ERROR', [('Content-Type', 'text/html')])
        return [msg]
        
        
def serve_fs(fs, indexes=True):
    """Serves an FS object via WSGI"""
    application = WSGIServer(fs, indexes)
    return application     
            
