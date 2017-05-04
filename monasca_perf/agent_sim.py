import argparse
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
import yaml

# path to the monasca-agent configuration file
AGENT_CONF_PATH = '/etc/monasca/agent/agent.yaml'

# keystone data to get a token. Can be set manually
# or read from the agent.yaml config file
keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    'auth_url': 'http://192.168.10.5:35357/v3'
}

# Run as "python agent_simulator.py

log = logging.getLogger(__name__)


processors = []  # global list to facilitate clean signal handling
exiting = False


def _get_agent_conf():
    """ get the monasca agent config"""
    with open(AGENT_CONF_PATH, 'r') as ac:
        return yaml.load(ac.read())


def _get_keystone_from_agent_conf():
    ac = _get_agent_conf()
    return {
        'username': ac['Api']['username'],
        'password': ac['Api']['password'],
        'project': ac['Api']['project_name'],
        'auth_url': ac['Api']['keystone_url'],
    }


def _get_monasca_api_url_from_agent_conf():
    ac = _get_agent_conf()
    return ac['Api']['url']

keystone = _get_keystone_from_agent_conf()


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
    """
    Simulate a monasca agent
    The process will report the number of metrics for each batch request to
    the q, it will also send exceptions it encounters. If no queue is provided,
    it will print these instead.
    """
    def __init__(self, proc_num, args, api_url, keystone_dict, queue=None,
                 metric_creator=MetricCreatorSimple, token=None):
        self.proc_num = proc_num
        self.num_requests = args.requests
        self.num_metrics = args.metrics_per_request
        self.interval = args.interval
        self.continuous = args.continuous
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


def parse_args():
    """parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--continuous', action='store_true',
                        help='Run in continuous mode (default: %(default)s)')
    parser.add_argument('--processes', type=int, default=1,
                        help='Number of processes. processes*requsts='
                        'the number of http connections (default: %(default)s)')
    parser.add_argument('--metrics-per-request', type=int, default=100,
                        help='Number of metrics per request '
                        '(default: %(default)s)')
    parser.add_argument('--requests', type=int, default=2,
                        help='Number of requests sent per interval. '
                        'processes*requsts=the number of http connections. '
                        '(default: %(default)s)')
    parser.add_argument('--interval', type=int, default=60,
                        help='The seconds to wait to send metrics. '
                        '(default: %(default)s)')
    parser.add_argument('--monasca-api-url', type=str, default=None,
                        help='The url to the monasca-api. If None, will be '
                        'autodetected from agent.yaml config. '
                        '(default: %(default)s)')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    print ("total metrics sent (per interval) = %d" % (args.processes * args.requests * args.metrics_per_request))
    print ("total connections (per interval) = %d" % (args.processes * args.requests))

    if args.monasca_api_url:
        api_url = args.monasca_api_url
    else:
        api_url = _get_monasca_api_url_from_agent_conf()

    for x in xrange(0, args.processes):
        p = multiprocessing.Process(
            target=agent_sim_process(x, args, api_url, keystone).run
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
