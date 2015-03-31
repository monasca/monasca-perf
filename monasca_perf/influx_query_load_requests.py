import simplejson as json
import requests, sys, time, multiprocessing, threading

# import logging, httplib
# httplib.HTTPConnection.debuglevel = 1
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

numMetrics = int(sys.argv[1])
numProcesses = int(sys.argv[2])
authToken = sys.argv[3]
url = sys.argv[4]
#url = "https://mon-api.rndd.aw1.hpcloud.net"

headers = {'Content-Type': 'application/json','X-Auth-Token': authToken,'X-Tenant-Id': '10462977980574'}
unixtime = int(time.time())
data = {'name': 'test_metric', 'timestamp': unixtime, 'value': 0}
r = requests.get(url, headers=headers, data=json.dumps(data))
print r.status_code
print r.request.headers
