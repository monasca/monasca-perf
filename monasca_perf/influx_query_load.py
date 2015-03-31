import simplejson as json
import pycurl, sys, time, multiprocessing, threading

numProcesses = int(sys.argv[1])
numQueriesPerProcess = int(sys.argv[2])
series = sys.argv[3]
url = sys.argv[4]

print numProcesses * numQueriesPerProcess

data = "q=select value from "
data += series

print data

def processWorker(numQueriesPerProcess):
    inc = 0
    while inc < numQueriesPerProcess:
        c = pycurl.Curl()
        c.setopt(pycurl.URL, url)
        c.setopt(pycurl.CURLOPT_NOSIGNAL, 1)
        c.setopt(pycurl.HTTPHEADER, ['Accept: text/plain'])
        c.setopt(pycurl.POSTFIELDS, data)
        c.setopt(c.VERBOSE, True)
        c.setopt(pycurl.USERPWD, 'rdd-admin:password')
        c.perform()
        response = c.getinfo(c.RESPONSE_CODE)
        if response != 200:
            print response, data
        print inc
        inc += 1
    

def processStart(numProcesses,numQueriesPerProcess):
    if __name__ == '__main__':
        jobs = []
        for i in range(numProcesses):
            p = multiprocessing.Process(target=processWorker, args=(numQueriesPerProcess,))
            jobs.append(p)
            p.start()


processStart(numProcesses,numQueriesPerProcess)

