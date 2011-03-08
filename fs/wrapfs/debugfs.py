'''
    @author: Marek Palatinus <marek@palatinus.cz>
    @license: Public domain
    
    DebugFS is a wrapper around filesystems to help developers
    debug their work. I wrote this class mainly for debugging
    TahoeLAFS and for fine tuning TahoeLAFS over Dokan with higher-level
    aplications like Total Comander, Winamp etc. Did you know
    that Total Commander need to open file before it delete them? :-)
    
    I hope DebugFS can be helpful also for other filesystem developers,
    especially for those who are trying to implement their first one (like me).
    
    DebugFS prints to stdout (by default) all attempts to
    filesystem interface, prints parameters and results.
    
    Basic usage:
        fs = DebugFS(OSFS('~'), identifier='OSFS@home', \
                skip=('_lock', 'listdir', 'listdirinfo'))
        print fs.listdir('.')
        print fs.unsupportedfunction()
    
    Error levels:
        DEBUG: Print everything (asking for methods, calls, response, exception)
        INFO: Print calls, responses, exception
        ERROR: Print only exceptions 
        CRITICAL: Print only exceptions not derived from fs.errors.FSError
        
    How to change error level:
        import logging
        logger = logging.getLogger('fs.debugfs')
        logger.setLevel(logging.CRITICAL)
        fs = DebugFS(OSFS('~')
        print fs.listdir('.')
    
'''
import logging
from logging import DEBUG, INFO, ERROR, CRITICAL
import sys

import fs
from fs.errors import FSError

logger = fs.getLogger('fs.debugfs')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

class DebugFS(object):
    def __init__(self, fs, identifier=None, skip=(), verbose=True):
        '''
            fs - Reference to object to debug
            identifier - Custom string-like object will be added
                to each log line as identifier.
            skip - list of method names which DebugFS should not log
        '''        
        self.__wrapped_fs = fs
        self.__identifier = identifier
        self.__skip = skip
        self.__verbose = verbose 
        super(DebugFS, self).__init__()
        
    def __log(self, level, message):
        if self.__identifier:
            logger.log(level, '(%s) %s' % (self.__identifier, message))
        else:            
            logger.log(level, message)
        
    def __parse_param(self, value):
        if isinstance(value, basestring):
            if len(value) > 60:
                value = "%s ... (length %d)" % (repr(value[:60]), len(value))
            else:
                value = repr(value) 
        elif isinstance(value, list):
            value = "%s (%d items)" % (repr(value[:3]), len(value))
        elif isinstance(value, dict):
            items = {}
            for k, v in value.items()[:3]:
                items[k] = v
            value = "%s (%d items)" % (repr(items), len(value))
        else:
            value = repr(value)
        return value
    
    def __parse_args(self, *arguments, **kwargs):
        args = [self.__parse_param(a) for a in arguments]
        for k, v in kwargs.items():
            args.append("%s=%s" % (k, self.__parse_param(v)))
        
        args = ','.join(args)
        if args: args = "(%s)" % args
        return args
        
    def __report(self, msg, key, value, *arguments, **kwargs):
        if key in self.__skip: return
        args = self.__parse_args(*arguments, **kwargs)
        value = self.__parse_param(value)
        self.__log(INFO, "%s %s%s -> %s" % (msg, str(key), args, value))
    
    def __getattr__(self, key):
        
        if key.startswith('__'):
            # Internal calls, nothing interesting
            return object.__getattribute__(self, key)

        try:
            attr = getattr(self.__wrapped_fs, key)
        except AttributeError, e:
            self.__log(DEBUG, "Asking for not implemented method %s" % key)
            raise e
        except Exception, e:
            self.__log(CRITICAL, "Exception %s: %s" % \
                     (e.__class__.__name__, str(e)))
            raise e
                
        if not callable(attr):
            if key not in self.__skip:
                self.__report("Get attribute", key, attr)
            return attr
        
        def _method(*args, **kwargs):
            try:
                value = attr(*args, **kwargs)                
                self.__report("Call method", key, value, *args, **kwargs)
            except FSError, e:
                self.__log(ERROR, "Call method %s%s -> Exception %s: %s" % \
                             (key, self.__parse_args(*args, **kwargs), \
                             e.__class__.__name__, str(e)))
                (exc_type,exc_inst,tb) = sys.exc_info()
                raise e, None, tb
            except Exception, e:
                self.__log(CRITICAL,
                         "Call method %s%s -> Non-FS exception %s: %s" %\
                         (key, self.__parse_args(*args, **kwargs), \
                         e.__class__.__name__, str(e)))
                (exc_type,exc_inst,tb) = sys.exc_info()
                raise e, None, tb
            return value
        
        if self.__verbose:
            if key not in self.__skip:
                self.__log(DEBUG, "Asking for method %s" % key)
        return _method
