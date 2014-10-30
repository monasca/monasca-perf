from urlparse import urlparse
from threading import Thread
import httplib, sys, multiprocessing
from Queue import Queue
import os
import signal
import simplejson
import json
import time
import logging
import logging.config

# Run as "python agent_simulator.py

RNDD_KAFKA0002 = 'http://15.184.17.107:8080/v2.0/metrics'
RNDD_KAFKA0003 = 'http://15.184.4.176:8080/v2.0/metrics'
RNDD_VIP = 'http://10.10.43.79:8080/v2.0/metrics'

# select which API URL to use
api_url = RNDD_KAFKA0002

# num_process x num_requests will be the number of http connections.  
# beware that 20,000 connections will cause too many ephemeral ports used
# on a single api server (with one ipaddress).  Would recommend not greater than 1000
num_processes = 500

# number of requests sent per interval (normally 1 if doing continuous)
num_requests = 1

# the agent sends anywhere between 40-360 metrics per request
num_metrics_per_request = 60

# (for continuous) The seconds to wait to send metrics. valid range 1-60 (lowest recommended is 10 by the agent)
interval = 10

# when False runs once, when True runs continuously sending num_requests every interval.
continuous = False

log = logging.getLogger(__name__)
print ("continuous = %d") % continuous
print ("using URL: %s") % api_url
print ("num_process = %d" % num_processes)
print ("num_metrics_per_request = %d" % num_metrics_per_request)
print ("num requests (sent per interval if continuous) = %d") % num_requests
print ("interval (secs) = %d" % interval)
print ("total metrics sent (per interval) = %d" % (num_processes * num_requests * num_metrics_per_request))
print ("total connections (per interval) = %d" % (num_processes * num_requests))

headers = {"Content-type": "application/json", "X-Auth-Token": "2685f55a60324c2ca6b5b4407d52f39a"}
url = urlparse(api_url)
processors = []  # global list to facilitate clean signal handling
exiting = False

class MetricPost():
    """Metrics Post process.
    
    """
    def __init__(self, proc_num, continuous=False):      
        self.proc_num = str(proc_num)
        self.continuous = continuous

    def doWorkContinuously(self):
        while(True):
            for x in xrange(num_requests):
                status,response=self.postMetrics()
                self.doSomethingWithResult(status,response)
            time.sleep(interval)
                
    def doWorkOnce(self):
        for x in xrange(num_requests):
            status,response=self.postMetrics()
            self.doSomethingWithResult(status,response)

    def postMetrics(self):
        try:
            conn = httplib.HTTPConnection(url.netloc)
            body = []
            for i in xrange(num_metrics_per_request):
                epoch = (int)(time.time()) - 120
                body.append({"name": "cube" + str(i), "dimensions": {"hostname": "server-" + self.proc_num}, "timestamp": epoch, "value": i})
            body = json.dumps(body, encoding='utf8')
            conn.request("POST", url.path, body, headers)
            res = conn.getresponse()
            if res.status != 204:
                raise Exception(res.status)
            return res.status, api_url
        except Exception as ex:
            print ex
            return "error", api_url

    def doSomethingWithResult(self, status, response):
        pass

    def run(self):
        if self.continuous:
            self.doWorkContinuously()
        else:
            self.doWorkOnce()


def clean_exit(signum, frame=None):
    """
    Exit all processes attempting to finish uncommited active work before exit.
    Can be called on an os signal or no zookeeper losing connection.
    """
    global exiting
    if exiting:
        # Since this is set up as a handler for SIGCHLD when this kills one child it gets another signal, the global
        # exiting avoids this running multiple times.
        log.debug('Exit in progress clean_exit received additional signal %s' % signum)
        return

    log.info('Received signal %s, beginning graceful shutdown.' % signum)
    exiting = True

    for process in processors:
        try:
            if process.is_alive():
                process.terminate()
        except Exception:
            pass

    # Kill everything, that didn't already die
    for child in multiprocessing.active_children():
        log.debug('Killing pid %s' % child.pid)
        try:
            os.kill(child.pid, signal.SIGKILL)
        except Exception:
            pass

    sys.exit(0)


if __name__ == '__main__':
    log.info('num_processes %d', num_processes)
    for x in xrange(0, num_processes):       
        p = multiprocessing.Process(
            target=MetricPost(x, continuous).run
        )
        processors.append(p)

    ## Start
    try:
        log.info('Starting processes')
        for process in processors:
            process.start()

        # The signal handlers must be added after the processes start otherwise they run on all processes
        signal.signal(signal.SIGCHLD, clean_exit)
        signal.signal(signal.SIGINT, clean_exit)
        signal.signal(signal.SIGTERM, clean_exit)

        log.info('calling Process.join() ')
        for process in processors:
            process.join()

    except Exception:
        log.exception('Error! Exiting.')
        for process in processors:
            process.terminate()
