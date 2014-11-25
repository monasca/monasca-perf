from urlparse import urlparse
from threading import Thread
import httplib, sys, multiprocessing
from Queue import Queue
import simplejson
import time
import base64

num_processes = int(sys.argv[1])
num_threads = int(sys.argv[2])
num_requests = int(sys.argv[3])
num_metrics_per_request = int(sys.argv[4])
series_name = sys.argv[5]
username = sys.argv[6]
password = sys.argv[7]

print num_processes * num_threads * num_requests * num_metrics_per_request

auth = base64.standard_b64encode('%s:%s' % (username,password)).replace('\n','')
authorization = "Basic "
authorization += auth
headers = {"Content-type": "application/json", "Authorization": authorization }

urls = [
    'http://localhost:8086/db/testmetrics/series'
]

def doWork(q):
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
        points = []
        for i in xrange(num_metrics_per_request):
            epoch = (int)(time.time()) - 120
            points.append([epoch,i])
        body.append({"name": series_name, "columns": ["timestamp", "value"], "points": points})
        body = simplejson.dumps(body)
        #print body
        conn.request("POST", url.path, body, headers)
        res = conn.getresponse()
        if res.status != 200:
            raise Exception(res.status)
        return res.status, ourl
    except Exception as ex:
        print ex
        return "error", ourl

def doSomethingWithResult(status, url):
    pass

def doProcess():
    q=Queue(num_threads)
    for i in range(num_threads):
        t=Thread(target=doWork, args=(q,))
        t.daemon=True
        t.start()
    try:
        for i in xrange(num_threads):
            url = urls[i%len(urls)]
            q.put(url.strip())
        q.join()
    except KeyboardInterrupt:
        sys.exit(1)

if __name__ == '__main__':
    jobs = []
    for i in range(num_processes):
        p = multiprocessing.Process(target=doProcess)
        jobs.append(p)
        p.start()
        p.join()

