"""num_threads simulates the number of clients
num_requests is the number of http requests per thread
num_metrics_per_request is the number of metrics per http request
Headers has the http header. You might want to set X-Auth-Token.
Urls can be an array of the Monasca API urls. There is only one url in
it right now, but you could add like the 3.
12/2014 Joe changes
- modified to create seperate python processes for each thread
1/2015 Allan changes 
- modified for QA performance baseline testing
- all metric posts create a new time series based on a unique name
- the default numbers simulate 10K agent nodes posting 100 metrics/agent node
- added token retrieval from keystone, happens once for entire run
- added timestamp to output
- moved keystone to node 01 from 09
- made metric body more realistic, added dimensions, simplified name 
6/2015 Allan changes
- modified for the CloudSystems env
- made dimensions as close to real CS metrics as possible
- by default, simulating 55K unique metrics to cover max CS load
- prior to running script, replace keystone password with password from hosts file on CS Monasca control node(s)
"""

import httplib
import multiprocessing
import sys
import time
import urlparse
from datetime import datetime
import simplejson
import hashlib

from xml.etree.ElementTree import XML

num_processes = 1
#num_requests = 1
#num_metrics_per_request = 1
num_requests = 220 # i - number of agents
num_metrics_per_request = 250 # x - number of metrics per agent

print "total: %s" % (num_processes*num_requests*num_metrics_per_request)
print('Time Stamp %s' % str(datetime.now()))

headers = {"Content-type": "application/json", "Accept": "application/json"}

urls = [
    'http://192.168.0.5:8080/v2.0/metrics'
]

keystone = 'http://192.168.0.20:5000/v3/auth/tokens'

def getToken():
        keyurl = urlparse.urlparse(keystone)
        keyconn = httplib.HTTPConnection(keyurl.netloc)
        keybody = { "auth": {
          "identity": {
          "methods": ["password"],
          "password": {
          "user": {
          "name": "monasca",
          "domain": { "id": "default" },
          "password": "538ac28040b1003fcaf259cb3c3fe51a2881a42b"
                  }
                      }
                      },
           "scope": {
           "project": {
           "name": "demo",
           "domain": { "id": "default" }
                      }
                    }
                               }
                    }
        keybody = simplejson.dumps(keybody)
        keyconn.request("POST", keyurl.path, keybody, headers)
        res = keyconn.getresponse()
        return res

def doWork(url_queue, num_requests,id):
    url = url_queue.get()
    for x in xrange(num_requests):
        status, response = getStatus(url,id,x)
        doSomethingWithResult(status, response)

def getStatus(ourl,id,x):
    try:
        url = urlparse.urlparse(ourl)
        conn = httplib.HTTPConnection(url.netloc)
        body = []
        for i in xrange(num_metrics_per_request):
            epoch = (int)(time.time()) - 120
            body.append({"name": "cs.test_perf_" + str(x),
                         "dimensions": {"service": "monitoring", "hostname": "kvm" + str(i) + "-cn.veritas.local", "host_type": "compute_node", "role": "kvm", "device": "hw-" + str(i), "nova_id": str(i)},
                         "timestamp": epoch*1000,
                         "value": i})
        body = simplejson.dumps(body)
        conn.request("POST", url.path, body, tokenheaders)
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
token = getToken().getheader('x-subject-token')
tokenheaders = {"Content-type": "application/json", "X-Auth-Token": token }
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
