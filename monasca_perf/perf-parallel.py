"""num_threads simulates the number of clients
num_requests is the number of http requests per thread
num_metrics_per_request is the number of metrics per http request
Headers has the http header. You might want to set X-Auth-Token.
Urls can be an array of the Monasca API urls. There is only one url in
it right now, but you could add like the 3.
12/2014 - Joe modified to create seperate python processes for each thread
1/2015 -  Allan modified for QA performance baseline testing
all metric posts create a new time series based on a unique name
the default numbers simulate 10K agent nodes posting 100 metrics/agent node
"""

import httplib
import multiprocessing
import sys
import time
import urlparse

import simplejson

num_processes = 100
num_requests = 100
num_metrics_per_request = 100

print "total: %s" % (num_processes*num_requests*num_metrics_per_request)

headers = {"Content-type": "application/json", "X-Auth-Token": "3134e6235306414480f48dc6674fba7d"}

urls = [
    'https://mon-ae1test-monasca01.useast.hpcloud.net:8080/v2.0/metrics',
    'https://mon-ae1test-monasca02.useast.hpcloud.net:8080/v2.0/metrics',
    'https://mon-ae1test-monasca03.useast.hpcloud.net:8080/v2.0/metrics',
]


def doWork(url_queue, num_requests,id):
    url = url_queue.get()
    for x in xrange(num_requests):
        status, response = getStatus(url,id,x)
        doSomethingWithResult(status, response)


def getStatus(ourl,id,x):
    try:
        url = urlparse.urlparse(ourl)
        conn = httplib.HTTPSConnection(url.netloc)
        body = []
        for i in xrange(num_metrics_per_request):
            epoch = (int)(time.time()) - 120
            body.append({"name": "perf-parallel-" + str(i) + "-" + str(x) + "-" + str(id),
                         "dimensions": {"dim-1": "value-1"},
                         "timestamp": epoch,
                         "value": i})
        body = simplejson.dumps(body)
        conn.request("POST", url.path, body, headers)
        res = conn.getresponse()
        if res.status != 204:
            raise Exception(res.status)
        return res.status, ourl
    except Exception as ex:
        print ex
        return "error", ourl


def doSomethingWithResult(status, url):
    pass

q = multiprocessing.Queue()
for i in xrange(num_processes):
    url = urls[i % len(urls)]
    q.put(url.strip())

process_list = []
for i in range(num_processes):
    p = multiprocessing.Process(target=doWork, args=(q, num_requests,i))
    process_list.append(p)
    p.start()

try:
    for p in process_list:
        try:
            p.join()
        except Exception:
            pass

except KeyboardInterrupt:
    sys.exit(1)
