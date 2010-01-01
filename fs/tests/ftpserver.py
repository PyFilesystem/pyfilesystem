try:
    from pyftpdlib import ftpserver
except ImportError:
    print "Requires pyftpdlib <http://code.google.com/p/pyftpdlib/>"
    raise
    
import sys

authorizer = ftpserver.DummyAuthorizer()
authorizer.add_user("user", "12345", sys.argv[1], perm="elradfmw")
authorizer.add_anonymous(sys.argv[1])

handler = ftpserver.FTPHandler
handler.authorizer = authorizer
address = ("127.0.0.1", 21)

ftpd = ftpserver.FTPServer(address, handler)
ftpd.serve_forever()

