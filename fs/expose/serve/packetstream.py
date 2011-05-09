try:
    from json import dumps, loads
except ImportError:
    from simplejson import dumps, loads
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


def encode(header='', payload=''):
    def textsize(s):
        if s:
            return str(len(s))
        return ''    
    return '%i,%i:%s%s' % (textsize(header), textsize(payload), header, payload)


class FileEncoder(object):
    
    def __init__(self, f):
        self.f = f
        
    def write(self, header='', payload=''):
        fwrite = self.f.write
        def textsize(s):
            if s:
                return str(len(s))
            return ''        
        fwrite('%s,%s:' % (textsize(header), textsize(payload)))
        if header:
            fwrite(header)
        if payload:
            fwrite(payload)


class JSONFileEncoder(FileEncoder):
    
    def write(self, header=None, payload=''):
        if header is None:
            super(JSONFileEncoder, self).write('', payload)
        else:         
            header_json = dumps(header, separators=(',', ':'))
            super(JSONFileEncoder, self).write(header_json, payload)


class DecoderError(Exception):
    pass

class PreludeError(DecoderError):
    pass

class Decoder(object):
    
    STAGE_PRELUDE, STAGE_SIZE, STAGE_HEADER, STAGE_PAYLOAD = range(4)
    MAX_PRELUDE = 255
        
    def __init__(self, no_prelude=False, prelude_callback=None):
        
        self.prelude_callback = prelude_callback
        self.stream_broken = False
        self.expecting_bytes = None
        self.stage = self.STAGE_PRELUDE
        self._prelude = []
        self._size = []
        self._expecting_bytes = None
        
        self.header_size = None
        self.payload_size = None
        
        self._header_bytes = None
        self._payload_bytes = None
        
        self._header_data = []
        self._payload_data = []
        
        self.header = None
        self.payload = None
        
        if no_prelude:
            self.stage = self.STAGE_SIZE
    
    
    def feed(self, data):
        
        if self.stream_broken:
            raise DecoderError('Stream is broken')
        
        STAGE_PRELUDE, STAGE_SIZE, STAGE_HEADER, STAGE_PAYLOAD = range(4)
                
        size_append = self._size.append
        header_append = self._header_data.append
        payload_append = self._payload_data.append
        datafind = data.find
        
        def reset_packet():
            self.expecting_bytes = None
            del self._header_data[:]
            del self._payload_data[:]
            self.header = None
            self.payload = None
            
        data_len = len(data)
        data_pos = 0
        expecting_bytes = self.expecting_bytes        
        stage = self.stage
        
        if stage == STAGE_PRELUDE:
            max_find = min(len(data), data_pos + self.MAX_PRELUDE)           
            cr_pos = datafind('\n', data_pos, max_find)
            if cr_pos == -1:
                self._prelude.append(data[data_pos:])
                data_pos = max_find
                if sum(len(s) for s in self._prelude) > self.MAX_PRELUDE:
                    self.stream_broken = True
                    raise PreludeError('Prelude not found')
            else:
                self._prelude.append(data[data_pos:cr_pos])
                if sum(len(s) for s in self._prelude) > self.MAX_PRELUDE:
                    self.stream_broken = True
                    raise PreludeError('Prelude not found')
                data_pos = cr_pos + 1                                
                prelude = ''.join(self._prelude)
                del self._prelude[:]
                reset_packet()
                if not self.on_prelude(prelude):
                    self.broken = True
                    return
                stage = STAGE_SIZE        
        
        while data_pos < data_len:
            
            if stage == STAGE_HEADER:
                bytes_to_read = min(data_len - data_pos, expecting_bytes)
                header_append(data[data_pos:data_pos + bytes_to_read])
                data_pos += bytes_to_read
                expecting_bytes -= bytes_to_read 
                if not expecting_bytes:                    
                    self.header = ''.join(self._header_data)                                    
                    if not self.payload_size:
                        yield self.header, ''
                        reset_packet()
                        expecting_bytes = None
                        stage = STAGE_SIZE
                    else:                        
                        stage = STAGE_PAYLOAD
                        expecting_bytes = self.payload_size
                                        
            elif stage == STAGE_PAYLOAD:
                bytes_to_read = min(data_len - data_pos, expecting_bytes)                
                payload_append(data[data_pos:data_pos + bytes_to_read])
                data_pos += bytes_to_read
                expecting_bytes -= bytes_to_read 
                if not expecting_bytes:                    
                    self.payload = ''.join(self._payload_data)
                    yield self.header, self.payload
                    reset_packet()
                    stage = STAGE_SIZE
                    expecting_bytes = None
                
            elif stage == STAGE_SIZE:
                term_pos = datafind(':', data_pos)
                if term_pos == -1:
                    size_append(data[data_pos:])                    
                    break
                else:
                    size_append(data[data_pos:term_pos])
                    data_pos = term_pos + 1                    

                size = ''.join(self._size)
                del self._size[:]
                if ',' in size:
                    header_size, payload_size = size.split(',', 1)
                else:
                    header_size = size
                    payload_size = ''                    
                try:
                    self.header_size = int(header_size or '0')
                    self.payload_size = int(payload_size or '0')
                except ValueError:
                    self.stream_broken = False
                    raise DecoderError('Invalid size in packet (%s)' % size)
                
                if self.header_size:
                    expecting_bytes = self.header_size                        
                    stage = STAGE_HEADER
                elif self.payload_size:
                    expecting_bytes = self.payload_size                        
                    stage = STAGE_PAYLOAD
                else:
                    # A completely empty packet, permitted, if a little odd
                    yield '', ''
                    reset_packet()                        
                    expecting_bytes = None

        self.expecting_bytes = expecting_bytes                
        self.stage = stage
                
                        
    def on_prelude(self, prelude):
        if self.prelude_callback and not self.prelude_callback(self, prelude):
            return False
        #pass
        #print "Prelude:", prelude
        return True


class JSONDecoder(Decoder):
    
    def feed(self, data):
        for header, payload in Decoder.feed(self, data):
            if header:
                header = loads(header)
            else:
                header = {}
            yield header, payload

   
if __name__ == "__main__":
    
    f = StringIO()
    encoder = JSONFileEncoder(f)
    encoder.write(dict(a=1, b=2), 'Payload')
    encoder.write(dict(foo="bar", nested=dict(apples="oranges"), alist=range(5)), 'Payload goes here')
    encoder.write(None, 'Payload')
    encoder.write(dict(a=1))
    encoder.write()
    
    stream = 'prelude\n' + f.getvalue()
    
    #print stream
    
#    packets = ['Prelude string\n',
#                encode('header', 'payload'),
#               encode('header number 2', 'second payload'),
#               encode('', '')]
#    
#    stream = ''.join(packets)
    
    decoder = JSONDecoder()
    
    stream = 'pyfs/0.1\n59,13:{"type":"rpc","method":"ping","client_ref":"-1221142848:1"}Hello, World!'
    
    fdata = StringIO(stream)
    
    while 1:
        data = fdata.read(3)
        if not data:
            break
        for header, payload in decoder.feed(data):
            print "Header:", repr(header)
            print "Payload:", repr(payload)   
        