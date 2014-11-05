"""num_threads simulates the number of clients
num_requests is the number of http requests per thread
num_metrics_per_request is the number of metrics per http request
Headers has the http header. You might want to set X-Auth-Token.
Urls can be an array of the Monasca API urls. There is only one url in it right now, but you could add like the 3.
"""
 
from urlparse import urlparse
from threading import Thread
import httplib, sys
from Queue import Queue
import simplejson
import time
 
num_threads = 100 
num_requests = 100 
num_metrics_per_request = 100 
 
print num_threads*num_requests*num_metrics_per_request
 
headers = {"Content-type": "application/json", "X-Auth-Token": "roland"}
 
urls = [
    'http://mon-aw1rdd1-kafka0002.rndd.aw1.hpcloud.net:8080/v2.0/metrics',
]
 
def doWork():
    url=q.get()
    for x in xrange(num_requests):
        status,response=getStatus(url)
        doSomethingWithResult(status,response)
    q.task_done()
 
def getStatus(ourl):
    try:
        url = urlparse(ourl)
        conn = httplib.HTTPConnection(url.netloc)
        body = []
        for i in xrange(num_metrics_per_request):
            epoch = (int)(time.time()) - 120
            body.append({"name": "test-" + str(i), "dimensions": {"dim-1": "value-1"}, "timestamp": epoch, "value": i})
            #body.append({"name": "test", "dimensions": {"dim-1": "value-1"}, "timestamp": epoch, "value": i})
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
 
q=Queue(num_threads)
 
for i in range(num_threads):
    t=Thread(target=doWork)
    t.daemon=True
    t.start()
try:
    for i in xrange(num_threads):
        url = urls[i%len(urls)]
        q.put(url.strip())
    q.join()
except KeyboardInterrupt:
    sys.exit(1)
