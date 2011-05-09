from __future__ import with_statement

import socket
import threading
from packetstream import JSONDecoder, JSONFileEncoder



class _SocketFile(object):
    def __init__(self, socket):
        self.socket = socket
        
    def read(self, size):
        try:
            return self.socket.recv(size)
        except socket.error:
            return ''
    
    def write(self, data):
        self.socket.sendall(data)

class ConnectionThread(threading.Thread):
    
    def __init__(self, server, connection_id, socket, address):
        super(ConnectionThread, self).__init__()
        self.server = server
        self.connection_id = connection_id        
        self.socket = socket
        self.transport = _SocketFile(socket)
        self.address = address        
        self.encoder = JSONFileEncoder(self.transport)
        self.decoder = JSONDecoder(prelude_callback=self.on_stream_prelude)        
    
        self._lock = threading.RLock()
        self.socket_error = None
            
        self.fs = None        
    
    def run(self):
        self.transport.write('pyfs/1.0\n')
        while True:
            try:
                data = self.transport.read(4096)
            except socket.error, socket_error:
                print socket_error
                self.socket_error = socket_error
                break
            print "data", repr(data)
            if data: 
                for packet in self.decoder.feed(data):                            
                    print repr(packet)
                    self.on_packet(*packet)
            else:
                break            
        self.on_connection_close()
            
    def close(self):
        with self._lock:
            self.socket.close()
    
    def on_connection_close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.server.on_connection_close(self.connection_id)
    
    def on_stream_prelude(self, packet_stream, prelude):
        print "prelude", prelude
        return True
    
    def on_packet(self, header, payload):
        print '-' * 30
        print repr(header)
        print repr(payload)            
        
        if header['method'] == 'ping':
            self.encoder.write({'client_ref':header['client_ref']}, payload)
         

class Server(object):
    
    def __init__(self, addr='', port=3000):
        self.addr = addr
        self.port = port
        self.socket = None
        self.connection_id = 0
        self.threads = {}
        self._lock = threading.RLock()
    
    def serve_forever(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.addr, self.port))        
        
        sock.listen(5)
        
        try:
            while True:
                clientsocket, address = sock.accept()
                self.on_connect(clientsocket, address)
        except KeyboardInterrupt:
            pass
        
        try:
            self._close_graceful()
        except KeyboardInterrupt:
            self._close_harsh()
        
    def _close_graceful(self):
        """Tell all threads to exit and wait for them"""
        with self._lock:
            for connection in self.threads.itervalues():
                connection.close()        
            for connection in self.threads.itervalues():
                connection.join()
            self.threads.clear()
            
    def _close_harsh(self):
        with self._lock:
            for connection in self.threads.itervalues():
                connection.close()
            self.threads.clear()
                          
    def on_connect(self, clientsocket, address):
        print "Connection from", address
        with self._lock:
            self.connection_id += 1
            thread = ConnectionThread(self,
                                      self.connection_id,
                                      clientsocket,
                                      address)            
            self.threads[self.connection_id] = thread            
            thread.start()            
        
    def on_connection_close(self, connection_id):
        pass
        #with self._lock:
        #    self.threads[connection_id].join()
        #    del self.threads[connection_id]
            
if __name__ == "__main__":
    server = Server()
    server.serve_forever()
    