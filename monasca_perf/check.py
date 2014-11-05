import httplib
import sys
import json
import time

# Use this when we get the ports open between the boxes
hosts = ['mon-aw1rdd1-influxdb0001.rndd.aw1.hpcloud.net', 'mon-aw1rdd1-influxdb0002.rndd.aw1.hpcloud.net',
         'mon-aw1rdd1-influxdb0003.rndd.aw1.hpcloud.net']
hosts = ['mon-aw1rdd1-influxdb0003.rndd.aw1.hpcloud.net']

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
            name = 'monasca.persister.pipeline.event.MetricHandler[%d].metric-definition-dimensions-added-to-batch-counter' % i
            value = counters[name]['count']
            if i in counts[host]:
                delta = value - counts[host][i]
                totalDelta += delta
                print("%d: total = %10.1d delta = %6.1d in %d seconds" % (i, value, delta, wait_time))
            counts[host][i] = value
            total += value
        print("Total for all threads %d delta %d" % (total, totalDelta))
    time.sleep(wait_time)
