import threading
import Queue as queue

def make_job(job_callable, *args, **kwargs):
    """ Returns a callable that calls the supplied callable with given arguements. """
    def job():
        return job_callable(*args, **kwargs)
    return job


class _PoolThread(threading.Thread):
    """ Internal thread class that runs jobs. """
    
    def __init__(self, queue, name):
        super(_PoolThread, self).__init__()
        self.queue = queue
        self.name = name
        
    def __str__(self):
        return self.name
        
    def run(self):
                
        while True:
            try:
                _priority, job = self.queue.get()                
            except queue.Empty:                
                break
            
            if job is None:            
                break
            
            if callable(job):
                try:
                    job()                
                except Exception, e:
                    print e                    
            self.queue.task_done()
    

class ThreadPool(object):
    
    def __init__(self, num_threads, size=None, name=''):
        
        self.num_threads = num_threads
        self.name = name
        self.queue = queue.PriorityQueue(size)
        self.job_no = 0
        
        self.threads =  [_PoolThread(self.queue, '%s #%i' % (name, i)) for i in xrange(num_threads)]
        
        for thread in self.threads:
            thread.start()
                
    def _make_priority_key(self, i):
        no = self.job_no
        self.job_no += 1
        return (i, no)

    def job(self, job_callable, *args, **kwargs):
        """ Post a job to the queue. """
        def job():
            return job_callable(*args, **kwargs)
        self.queue.put( (self._make_priority_key(1), job), True )
        return self.job_no        
        
    def flush_quit(self):
        """ Quit after all tasks on the queue have been processed. """        
        for thread in self.threads:
            self.queue.put( (self._make_priority_key(1), None) )        
        for thread in self.threads:
            thread.join()
        
    def quit(self):
        """ Quit as soon as possible, potentially leaving tasks on the queue. """
        for thread in self.threads:
            self.queue.put( (self._make_priority_key(0), None) )            
        for thread in self.threads:
            thread.join()


if __name__ == "__main__":
    import time
    

    def job(n):
        print "Starting #%i" % n
        time.sleep(1)
        print "Ending #%i" % n
    
    pool = ThreadPool(5, 'test thread')
    
    for n in range(20):
        pool.job(job, n)
    
    pool.flush_quit()

        
    