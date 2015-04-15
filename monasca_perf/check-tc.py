# outputs Monasca persister statistics from port 8091 every 5 seconds
# 1/2015 - Allan modified
# pointed to TestCluster1 nodes
# added timestamp for calculating time for all metrics pushed into influxdb

import httplib
import sys
import json
import time
from datetime import datetime

# Use this when we get the ports open between the boxes
hosts = ['mon-ae1test-monasca01.useast.hpcloud.net', 
         'mon-ae1test-monasca02.useast.hpcloud.net',
         'mon-ae1test-monasca03.useast.hpcloud.net']

counts = {}
connections = [ (host, httplib.HTTPConnection(host, 8091)) for host in hosts]
metrics = {}
wait_time = 5
while True:
    for (host, connection) in connections:
        connection.request('GET', '/metrics')
        resp = connection.getresponse()
        if resp.status != 200:
            print("Failure %d" % resp.status)
            sys.exit(1)
        if host not in counts:
            counts[host] = {}
        print('%s:' % host)
        data = resp.read()
        counters = json.loads(data)['counters']
        total = 0
        totalDelta = 0
        for i in range(0, 4):
            name = 'monasca.persister.pipeline.event.MetricHandler[%d].metrics-added-to-batch-counter' % i
            value = counters[name]['count']
            if i in counts[host]:
                delta = value - counts[host][i]
                totalDelta += delta
                print("%d: total = %10.1d delta = %6.1d in %d seconds" % (i, value, delta, wait_time))
            counts[host][i] = value
            total += value
        print("Total for all threads %d delta %d" % (total, totalDelta))
        print('Time Stamp %s' % str(datetime.now()))
    time.sleep(wait_time)
