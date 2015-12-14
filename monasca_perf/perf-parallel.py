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
import random
import sys
import time
import urlparse

import simplejson
import socket

from monascaclient import ksclient

num_processes = 1

monasca_url = 'http://mn-ccp-vip-MON-API-mgmt:8070/v2.0'


def getToken():
    keystone = {
        'username': 'monasca-agent',
        'password': 'ucy7ycVurehC',
        'project_id': 'null',
        'auth_url':  'http://mn-ccp-vip-KEY-API-mgmt:5000/v3'
    }

    ks_client = ksclient.KSClient(**keystone).token
    return ks_client.token


def post_metrics(id, ourl):
    headers = {"Content-type": "application/json",
               "X-Auth-Token": getToken()}
    while True:
        try:
            url = urlparse.urlparse(ourl)
            conn = httplib.HTTPConnection(url.netloc)
            body = []
            for i in xrange(1310):
                epoch = (int)(time.time()) - 120
                body.append({"name": "perf-parallel-" + str(i) + "-" + str(id),
                             "dimensions": {"perf-id": str(id),
                                            "zone": "nova",
                                            "service": "compute",
                                            "resource_id": "34c0ce14-9ce4-4d3d-84a4-172e1ddb26c4",
                                            "tenant_id": "71fea2331bae4d98bb08df071169806d",
                                            "hostname": socket.gethostname(),
                                            "component": "vm",
                                            "control_plane": "ccp",
                                            "amplifier": "",
                                            "cluster": "compute",
                                            "cloud_name": "monasca"},
                             "timestamp": epoch * 1000,
                             "value": i})
            body = simplejson.dumps(body)
            conn.request("POST", url.path, body, headers)
            res = conn.getresponse()
            if res.status == 401:
                headers['X-Auth-Token'] = getToken()
            if res.status != 204:
                raise Exception(res.status)
            time.sleep(random.randint(20, 30))
        except Exception as ex:
            print(ex)
            return "error", ourl


def doSomethingWithResult(status, url):
    pass

process_list = []
for i in range(num_processes):
    p = multiprocessing.Process(target=post_metrics, args=(i, monasca_url))
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
