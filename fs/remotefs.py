# Work in Progress - Do not use

from fs.base import FS
from fs.expose.serve import packetstream

from collections import defaultdict
import threading
from threading import Lock, RLock
from json import dumps
import Queue as queue
import socket

class PacketHandler(threading.Thread):
    
    def __init__(self, transport, prelude_callback=None):
        super(PacketHandler, self).__init__()
        self.transport = transport
        self.encoder = packetstream.JSONFileEncoder(transport)
        self.decoder = packetstream.JSONDecoder(prelude_callback=None)
        
        self.queues = defaultdict(queue.Queue)            
        self._encoder_lock = threading.Lock()
        self._queues_lock = threading.Lock()
        self._call_id_lock = threading.Lock()
        
        self.call_id = 0
                
    def run(self):
        decoder = self.decoder
        read = self.transport.read
        on_packet = self.on_packet
        while True:
            data = read(1024*16)
            if not data:
                print "No data"
                break
            print "data", repr(data)            
            for header, payload in decoder.feed(data):
                print repr(header)
                print repr(payload)
                on_packet(header, payload)
             
    def _new_call_id(self):
        with self._call_id_lock:
            self.call_id += 1
            return self.call_id
             
    def get_thread_queue(self, queue_id=None):
        if queue_id is None:
            queue_id = threading.current_thread().ident
        with self._queues_lock:
            return self.queues[queue_id]
             
    def send_packet(self, header, payload=''):
        call_id = self._new_call_id()                
        queue_id = threading.current_thread().ident        
        client_ref = "%i:%i" % (queue_id, call_id)        
        header['client_ref'] = client_ref
        
        with self._encoder_lock:
            self.encoder.write(header, payload)
            
        return call_id
        
    def get_packet(self, call_id):
        
        if call_id is not None:
            queue_id = threading.current_thread().ident        
            client_ref = "%i:%i" % (queue_id, call_id)
        else:
            client_ref = None
        
        queue = self.get_thread_queue()
        
        while True:        
            header, payload = queue.get()
            print repr(header)
            print repr(payload)
            if client_ref is not None and header.get('client_ref') != client_ref:
                continue
            break                
        
        return header, payload
                
    def on_packet(self, header, payload):
        client_ref = header.get('client_ref', '')
        queue_id, call_id = client_ref.split(':', 1)
        queue_id = int(queue_id)
        #queue_id = header.get('queue_id', '')
        queue = self.get_thread_queue(queue_id)        
        queue.put((header, payload))
     
     
class _SocketFile(object):
    def __init__(self, socket):
        self.socket = socket
        
    def read(self, size):
        try:
            return self.socket.recv(size)
        except:           
            return ''        
    
    def write(self, data):
        self.socket.sendall(data)
        
    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        

class RemoteFS(FS):
    
    def __init__(self, addr='', port=3000, transport=None):
        self.addr = addr
        self.port = port
        self.transport = transport
        if self.transport is None:
            self.transport = self._open_connection()
        self.packet_handler = PacketHandler(self.transport)  
        self.packet_handler.start()  
        
    def _open_connection(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.addr, self.port))
        socket_file = _SocketFile(sock)
        socket_file.write('pyfs/0.1\n')
        return socket_file
        
    def ping(self, msg):
        call_id = self.packet_handler.send_packet({'type':'rpc', 'method':'ping'}, msg)
        header, payload = self.packet_handler.get_packet(call_id)
        print "PING"
        print header
        print payload
    
    def close(self):
        self.transport.close()
        self.packet_handler.join()
        

if __name__ == "__main__":
    
    rfs = RemoteFS()
    rfs.ping("Hello, World!")
    rfs.close()
    