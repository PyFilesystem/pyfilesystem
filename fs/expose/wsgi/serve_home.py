from wsgiref.simple_server import make_server

from fs.osfs import OSFS
from wsgi import serve_fs
osfs = OSFS('~/')
application = serve_fs(osfs)

httpd = make_server('', 8000, application)
print "Serving on http://127.0.0.1:8000"
httpd.serve_forever()
