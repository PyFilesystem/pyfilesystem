from fs.osfs import OSFS
from wsgi import serve_fs
osfs = OSFS('~/')
application = serve_fs(osfs)