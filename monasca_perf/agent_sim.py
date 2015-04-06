from urlparse import urlparse
import httplib, sys, multiprocessing
import os
import signal
import json
import time
from datetime import datetime
import logging
import logging.config
from monascaclient import ksclient
from monascaclient import client

keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    'auth_url': 'http://192.168.10.5:35357/v3'
}

# Run as "python agent_simulator.py

RNDD_KAFKA0002 = 'http://127.0.0.1:8080/v2.0/metrics'
MINI_MON = 'http://192.168.10.4:8080/v2.0/'

# select which API URL to use
api_url = MINI_MON

# num_process x num_requests will be the number of http connections.  
# beware that 20,000 connections will cause too many ephemeral ports used
# on a single api server (with one ipaddress).  Would recommend not greater than 1000
num_processes = 1

# number of requests sent per interval (normally 1-20 max if doing continuous)
num_requests = 2

# the agent sends anywhere between 40-360 metrics per request
num_metrics_per_request = 100

# (for continuous) The seconds to wait to send metrics. valid range 1-60 (lowest recommended is 10 by the agent)
agent_interval = 60

# when False runs once, when True runs continuously sending num_requests every interval.
continuous = False

log = logging.getLogger(__name__)


processors = []  # global list to facilitate clean signal handling
exiting = False

class MetricCreatorSimple():
    """ Generates metrics
    """
    def __init__(self, proc_num):
        self.proc_num = proc_num
        self.num_calls = 0
        self.start_time = int((time.time() - 120)*1000)

    def create_metric(self):
        metric = {"name": "cube" + str(self.proc_num),
                  "dimensions": {"hostname": "server-" + str(self.proc_num)},
                  "timestamp": self.start_time+self.num_calls,
                  "value": self.num_calls}
        self.num_calls += 1
        return metric

class agent_sim_process():
    """Simulate a monasca agent
        arguments
            proc_num - identifying number for the agent
            num_requests - how many requests the agent makes per interval
            num_metrics - how many metrics are in each request
            continuous - run once or forever
            queue - (multiprocessing.Queue) if provided, agent will use to report number of metrics sent
            metric_creator - agent will call "create_metric" method from this object and will pass in proc_num
            token - what token should the agent use, will generate its own token if none provided

        The process will report the number of metrics for each batch request to the q, it will also send exceptions
        it encounters. If no queue is provided, it will print these instead.
    """
    def __init__(self, proc_num, num_requests, num_metrics, api_url, keystone_dict, continuous=False, interval=60, queue=None,
                 metric_creator=MetricCreatorSimple, token=None):
        self.proc_num = proc_num
        self.num_requests = num_requests
        self.num_metrics = num_metrics
        self.interval = interval
        self.continuous = continuous
        self.queue = queue
        if not token:
            try:
                token = ksclient.KSClient(**keystone_dict).token
            except Exception as ex:
                print("Agent {}: Failed to get auth token from keystone\n{}".format(self.proc_num, keystone_dict))
        #print("Using token: " + token)
        self.mon_client = client.Client('2_0', api_url, token=token)
        self.metric_creator = metric_creator(proc_num)
        #print("Created agent {}".format(self.proc_num))

    def do_work_continuously(self):
        while True:
            start_send = time.time()
            for x in xrange(self.num_requests):
                self.post_metrics()
            end_send = time.time()

            secs = end_send - start_send
            if secs < self.interval:
                sleep_interval = self.interval - secs
            else:
                sleep_interval = 0
                #print ("send seconds %f took longer than interval %f, not sleeping" % (secs, self.interval))
            #print ("send time = %f, sleep time = %f" % (secs, sleep_interval) )
            time.sleep(sleep_interval)
                
    def do_work_once(self):
        start_send = time.time()
        for x in xrange(self.num_requests):
            self.post_metrics()
            end_send = time.time()
        secs = end_send - start_send
        #print ("send time in seconds = %f" % (secs))

    def post_metrics(self):
        try:
            body = []
            for i in xrange(self.num_metrics):
                body.append(self.metric_creator.create_metric())
            self.mon_client.metrics.create(jsonbody=body)
            if self.queue:
                self.queue.put(self.num_metrics)
        except Exception as ex:
            if self.queue:
                self.queue.put(ex)
            else:
                print(ex)

    def run(self):
        if self.continuous:
            self.do_work_continuously()
        else:
            self.do_work_once()


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
    print ("continuous = %d") % continuous
    print ("using URL: %s") % api_url
    print ("num_process = %d" % num_processes)
    print ("num_metrics_per_request = %d" % num_metrics_per_request)
    print ("num requests (sent per interval if continuous) = %d") % num_requests
    print ("interval (secs) = %d" % agent_interval)
    print ("total metrics sent (per interval) = %d" % (num_processes * num_requests * num_metrics_per_request))
    print ("total connections (per interval) = %d" % (num_processes * num_requests))

    log.info('num_processes %d', num_processes)
    for x in xrange(0, num_processes):       
        p = multiprocessing.Process(
            target=agent_sim_process(x, num_requests, num_metrics_per_request, api_url, continuous,
                                     keystone, agent_interval).run
        )
        processors.append(p)

    ## Start
    try:
        log.info('Starting processes')
        print ('Starting processes %s' % str(datetime.now()))
        start = time.time()
        for process in processors:
            process.start()

        # The signal handlers must be added after the processes start otherwise they run on all processes
        signal.signal(signal.SIGCHLD, clean_exit)
        signal.signal(signal.SIGINT, clean_exit)
        signal.signal(signal.SIGTERM, clean_exit)

        log.info('calling Process.join() ')
        for process in processors:
            process.join()
        end = time.time()
        print ("runtime = %d seconds" % (end - start))
    except Exception:
        print ('Error! Exiting.')
        for process in processors:
            process.terminate()
        end = time.time()
        print ("runtime = %d seconds" % (end - start))
