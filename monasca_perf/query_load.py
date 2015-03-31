from urlparse import urlparse
import httplib, sys, multiprocessing

num_processes = int(sys.argv[1])
series_name = sys.argv[2]

print num_processes

headers = {"Content-type": "application/json", "X-Auth-Token": "HPAuth10_dfd2a6394b90b13c77cc06c68064ca5763b58ba7e05399d9ffe23d2df2524838" }

ourl = 'http://localhost:8086/db/testmetrics/series'

def doProcess():
    try:
        url = urlparse(ourl)
        conn = httplib.HTTPConnection(url.netloc)
        conn.request("POST", url.path, "", headers)
        res = conn.getresponse()
        if res.status != 200:
            raise Exception(res.status)
        return res.status, ourl
    except Exception as ex:
        print ex
        return "error", ourl

if __name__ == '__main__':
    jobs = []
    for i in range(num_processes):
        p = multiprocessing.Process(target=doProcess)
        jobs.append(p)
        p.start()
        p.join()

