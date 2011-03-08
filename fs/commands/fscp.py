#!/usr/bin/env python
from fs.utils import copyfile, copyfile_non_atomic
from fs.path import pathjoin, iswildcard
from fs.commands.runner import Command
import sys
import Queue as queue
import time
import threading


class FileOpThread(threading.Thread):            
    
    def __init__(self, action, name, dest_fs, queue, on_done, on_error):
        self.action = action                  
        self.dest_fs = dest_fs
        self.queue = queue
        self.on_done = on_done
        self.on_error = on_error
        self.finish_event = threading.Event()
        super(FileOpThread, self).__init__()        
    
    def run(self):        
        
        while not self.finish_event.isSet():            
            try:
                path_type, fs, path, dest_path = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:                
                if path_type == FScp.DIR:                        
                    self.dest_fs.makedir(path, recursive=True, allow_recreate=True)
                else:                                                                
                    self.action(fs, path, self.dest_fs, dest_path, overwrite=True)                    
            except Exception, e:                
                self.on_error(e)                                
                self.queue.task_done()                                  
                break                   
            else:
                self.queue.task_done() 
                self.on_done(path_type, fs, path, self.dest_fs, dest_path)
                      
                     

class FScp(Command):
    
    DIR, FILE = 0, 1
    
    usage = """fscp [OPTION]... [SOURCE]... [DESTINATION]
Copy SOURCE to DESTINATION"""
    
    def get_action(self):
        if self.options.threads > 1:
            return copyfile_non_atomic
        else:
            return copyfile
    
    def get_verb(self):
        return 'copying...'

    def get_optparse(self):
        optparse = super(FScp, self).get_optparse()
        optparse.add_option('-p', '--progress', dest='progress', action="store_true", default=False,
                            help="show progress", metavar="PROGRESS")
        optparse.add_option('-t', '--threads', dest='threads', action="store", default=1,
                            help="number of threads to use", type="int", metavar="THREAD_COUNT")        
        return optparse
        
    def do_run(self, options, args):
               
        self.options = options 
        if len(args) < 2:
            self.error("at least two filesystems required\n")
            return 1
        
        srcs = args[:-1]
        dst = args[-1] 
        
        dst_fs, dst_path = self.open_fs(dst, writeable=True, create_dir=True)
        
        if dst_path is not None and dst_fs.isfile(dst_path):
            self.error('Destination must be a directory\n')
            return 1
        
        if dst_path:
            dst_fs = dst_fs.makeopendir(dst_path)
            dst_path = None                      
                
        copy_fs_paths = []
        
        progress = options.progress                
        
        if progress:
            sys.stdout.write(self.progress_bar(len(srcs), 0, 'scanning...'))
            sys.stdout.flush()
        
        self.root_dirs = [] 
        for i, fs_url in enumerate(srcs):
            src_fs, src_path = self.open_fs(fs_url)                      

            if src_path is None:
                src_path = '/'

            if iswildcard(src_path):
                for file_path in src_fs.listdir(wildcard=src_path, full=True):
                    copy_fs_paths.append((self.FILE, src_fs, file_path, file_path))
                    
            else:                
                if src_fs.isdir(src_path): 
                    self.root_dirs.append((src_fs, src_path))                                        
                    src_sub_fs = src_fs.opendir(src_path)
                    for dir_path, file_paths in src_sub_fs.walk():
                        if dir_path not in ('', '/'):                                            
                            copy_fs_paths.append((self.DIR, src_sub_fs, dir_path, dir_path))
                        sub_fs = src_sub_fs.opendir(dir_path)
                        for file_path in file_paths:                                                         
                            copy_fs_paths.append((self.FILE, sub_fs, file_path, pathjoin(dir_path, file_path)))
                else:
                    if src_fs.exists(src_path):
                        copy_fs_paths.append((self.FILE, src_fs, src_path, src_path))
                    else:
                        self.error('%s is not a file or directory\n' % src_path)
                        return 1 
                    
            if progress:
                sys.stdout.write(self.progress_bar(len(srcs), i + 1, 'scanning...'))
                sys.stdout.flush()
                                       
        if progress:
            sys.stdout.write(self.progress_bar(len(copy_fs_paths), 0, self.get_verb()))
            sys.stdout.flush()
                                        
        if self.options.threads > 1:            
            copy_fs_dirs = [r for r in copy_fs_paths if r[0] == self.DIR]
            copy_fs_paths = [r for r in copy_fs_paths if r[0] == self.FILE]            
            for path_type, fs, path, dest_path in copy_fs_dirs:               
                dst_fs.makedir(path, allow_recreate=True, recursive=True)                             
        
        self.lock = threading.RLock()
            
        self.total_files = len(copy_fs_paths)
        self.done_files = 0
                
        file_queue = queue.Queue()        
        threads = [FileOpThread(self.get_action(),
                                'T%i' % i,
                                dst_fs,
                                file_queue,
                                self.on_done,
                                self.on_error)
                        for i in xrange(options.threads)]
        
        for thread in threads:
            thread.start()
        
        self.action_errors = []
        complete = False
        try:        
            enqueue = file_queue.put            
            for resource in copy_fs_paths:
                enqueue(resource)
                        
            while not file_queue.empty():
                time.sleep(0)
                if self.any_error():
                    raise SystemExit
            # Can't use queue.join here, or KeyboardInterrupt will not be
            # caught until the queue is finished 
            #file_queue.join()
        
        except KeyboardInterrupt:            
            options.progress = False                    
            self.output("\nCancelling...\n")
                
        except SystemExit:
            options.progress = False            
                               
        finally:
            sys.stdout.flush()                
            for thread in threads:
                thread.finish_event.set()                           
            for thread in threads:
                thread.join()
            complete = True
            if not self.any_error():
                self.post_actions()
                                   
        dst_fs.close()
        
        if self.action_errors:
            for error in self.action_errors:
                self.error(self.wrap_error(unicode(error)) + '\n')            
            sys.stdout.flush()
        else:
            if complete and options.progress:
                sys.stdout.write(self.progress_bar(self.total_files, self.done_files, ''))
                sys.stdout.write('\n')
                sys.stdout.flush()
        
    def post_actions(self):
        pass
        
    def on_done(self, path_type, src_fs, src_path, dst_fs, dst_path):        
        self.lock.acquire()        
        try:
            if self.options.verbose:
                if path_type == self.DIR:
                    print "mkdir %s" % dst_fs.desc(dst_path)
                else:
                    print "%s -> %s" % (src_fs.desc(src_path), dst_fs.desc(dst_path))
            elif self.options.progress:
                self.done_files += 1        
                sys.stdout.write(self.progress_bar(self.total_files, self.done_files, self.get_verb()))
                sys.stdout.flush()
        finally:
            self.lock.release()
            
    def on_error(self, e):        
        self.lock.acquire()
        try:
            self.action_errors.append(e)
        finally:
            self.lock.release()
    
    def any_error(self):                
        self.lock.acquire()
        try:
            return bool(self.action_errors)
        finally:
            self.lock.release()
            
    def progress_bar(self, total, remaining, msg=''):
        bar_width = 20
        throbber = '|/-\\'
        throb = throbber[remaining % len(throbber)]
        done = float(remaining) / total
        
        done_steps = int(done * bar_width)
        bar_steps = ('#' * done_steps).ljust(bar_width) 
                
        msg = '%s %i%%' % (msg, int(done * 100.0))        
        msg = msg.ljust(20)
        
        if total == remaining:
            throb = ''
            
        bar = '\r%s[%s] %s\r' % (throb, bar_steps, msg.lstrip())
        return bar
        
def run():
    return FScp().run()

if __name__ == "__main__":
    sys.exit(run())
