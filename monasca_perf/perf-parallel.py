"""num_threads simulates the number of clients
num_requests is the number of http requests per thread
num_metrics_per_request is the number of metrics per http request
Headers has the http header. You might want to set X-Auth-Token.
Urls can be an array of the Monasca API urls. There is only one url in
it right now, but you could add like the 3.
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

headers = {"Content-type": "application/json", "X-Auth-Token": "roland"}

urls = [
    'http://mon-aw1rdd1-kafka0002.rndd.aw1.hpcloud.net:8080/v2.0/metrics',
]


def doWork(url_queue, num_requests):
    url = url_queue.get()
    for x in xrange(num_requests):
        status, response = getStatus(url)
        doSomethingWithResult(status, response)


def getStatus(ourl):
    try:
        url = urlparse.urlparse(ourl)
        conn = httplib.HTTPConnection(url.netloc)
        body = []
        for i in xrange(num_metrics_per_request):
            epoch = (int)(time.time()) - 120
            body.append({"name": "test-" + str(i),
                         "dimensions": {"dim-1": "value-1"},
                         "timestamp": epoch,
                         "value": i})
            # body.append({"name": "test",
            #              "dimensions": {"dim-1": "value-1"},
            #              "timestamp": epoch,
            #              "value": i})
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
    p = multiprocessing.Process(target=doWork, args=(q, num_requests))
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
