import time
import spur
import json
import logging
import pycurl
import threading
from StringIO import StringIO

errorRetry = 50
numThreads = 20

class InfluxEnv(object):
    def __init__(self,ip1,ip2,ip3,username,password,pem):
        self.errorRetry = errorRetry
        self.numThreads = numThreads
        self.ip = [ "", ip1, ip2, ip3 ]
        if username is None:
            self.username = ""
        else:
            self.username = username
        if password is None:
            self.password = ""
        else:
            self.password = password
        if pem is None:
            self.pem = ""
        else:
            self.pem = pem
        self.datetime = time.strftime("%Y_%m_%dT%H_%M_%S", time.gmtime())
        self.db = 'run_' + self.datetime
        logging.basicConfig(filename='/tmp/'+self.db,level=logging.INFO)
        logging.info(self.db)
        logging.info("")
    def testLogBanner(self,test_name):
        logging.info("")
        logging.info("Test: "+test_name)
        logging.info("----------------------------------------------------------------------------------------")
        logging.info("")
    def executeCommand(self,node,command,pause):
        logging.info("executeCommand")
        logging.info("Node: "+str(node))
        logging.info(command)
        result_text = ""
        try:
            if len(self.password) > 0:
                shell = spur.SshShell(hostname=self.ip[node],username=self.username,password=self.password)
            else:
                shell = spur.SshShell(hostname=self.ip[node],username=self.username,private_key_file=self.pem)
            with shell:
                result = shell.run(command)
                result_text = result.output
            #print result.output
        except spur.results.RunProcessError:
            pass
        logging.info("Result:"+result_text)
        time.sleep(pause)
        return result_text
    def stopInflux(self,node):
        logging.info("stopInflux")
        self.executeCommand(node,["sudo","service","influxdb","stop"],2)
    def startInflux(self,node):
        logging.info("startInflux")
        self.executeCommand(node,["sudo","service","influxdb","start"],8)
    def killInflux(self,node):
        logging.info("killInflux")
        self.executeCommand(node,["sudo","pkill","influxdb"],1)
    def createDB(self):
        logging.info("createDB")
        self.executeCommand(1,["sh","-c",'curl -G http://localhost:8086/query --data-urlencode "q=CREATE DATABASE ' + self.db + '"'],0)
        self.executeCommand(1,["sh","-c",'curl -G http://localhost:8086/query?pretty=true --data-urlencode "q=CREATE RETENTION POLICY mypolicy ON ' + self.db + ' DURATION 90d REPLICATION 3 DEFAULT"'],0)
        print "DB Name:", self.db
    def allPartitionStart(self):
        for n in range(1,3):
            self.partitionStart(n)
    def partitionStart(self,node):
        self.executeCommand(node,["sudo","ufw","allow","22"],2)
        self.executeCommand(node,["sh","-c","echo y | sudo ufw enable"],2)
    def allPartitionStop(self):
        for n in range(1,3):
            self.partitionStop(n)
    def partitionStop(self,node):
        self.executeCommand(node,["sh","-c","echo y | sudo ufw reset"],2)
    def fullyPartitionNode(self,node):
        #This fully partitions a node away from everything
        self.executeCommand(node,["sudo","ufw","deny","8086"],2)
        self.executeCommand(node,["sudo","ufw","deny","out","8086"],2)
    def singlePartitionNode(self,node1,node2):
        #This is just between 2 nodes, not from everything
        self.executeCommand(node1,["sudo","ufw","deny","from",self.ip[node2],"to","any","port","8086"],2)
        self.executeCommand(node1,["sudo","ufw","deny","out","from",self.ip[node2],"to","any","port","8086"],2)
    def unidirectionalSinglePartitionNode(self,node1,node2):
        #This is just between 2 nodes, not from everything; unidirectional, node1 cannot receive from node2
        self.executeCommand(node1,["sudo","ufw","deny","from",self.ip[node2],"to","any","port","8086"],2)
    def sendSingleMetric(self,node,tsname,value):
        logging.info("sendSingleMetric")
        for i in range(0,self.errorRetry):
            result = self.executeCommand(node,["sh","-c",'curl -X POST http://localhost:8086/write -d \' { "database": "' + self.db + '", "retentionPolicy": "mypolicy", "points": [ { "name": "' + tsname + '", "fields": { "value": ' + str(value) + ' } } ] }\''],2)
            if result.find("error") != -1:
                time.sleep(10)
                continue
            break
    def sendMultipleMetricsThread(self,url,payload,count):
        for i in range(0,count):
            for j in range(0,self.errorRetry):
                cbuffer = StringIO()
                c = pycurl.Curl()
                c.setopt(pycurl.URL,url )
                c.setopt(pycurl.HTTPHEADER, ['X-Postmark-Server-Token: API_TOKEN_HERE','Accept: application/json'])
                c.setopt(pycurl.POST, 1)
                c.setopt(c.WRITEDATA, cbuffer)
                c.setopt(pycurl.POSTFIELDS, payload)
                c.perform()
#                 if len(cbuffer.getvalue()) >0: print buffer.getvalue()
                c.close()
                if cbuffer.getvalue().find("error") != -1:
#                     logging.info("sendMultipleMetricsThread: Error Retry "+j)
                    if j >= self.errorRetry - 1:
                        logging.info("sendMultipleMetricsThread: Max Error Retry Failure")
                    time.sleep(1)
                    continue
                break
    def sendMultipleMetrics(self,node,tsname,count):
        logging.info("sendMultipleMetrics")
        payload = '{ "database": "' + self.db + '", "retentionPolicy": "mypolicy", "points": [ { "name": "' + tsname + '", "fields": { "value": -1 }}]}'
        url = 'http://' + self.ip[node] + ':8086/write'
        countForThread = int(count / self.numThreads)
        threadList = []
        for i in range(0,self.numThreads):
            t = threading.Thread(target=self.sendMultipleMetricsThread, args=(url,payload,countForThread))
            t.start()
            threadList.append(t)
        for t in threadList:
            t.join()
    def listMetrics(self,node,tsname):
        logging.info("listMetrics")
        return self.executeCommand(node,["sh","-c",'curl -G http://localhost:8086/query?pretty=true --data-urlencode "db=' + self.db + '" --data-urlencode "q=SELECT * FROM ' + tsname + '"'],0)
    def countMetrics(self,node,tsname):
        logging.info("countMetrics")
        for i in range(0,self.errorRetry):
            result = self.executeCommand(node,["sh","-c",'curl -G http://localhost:8086/query?pretty=true --data-urlencode "db=' + self.db + '" --data-urlencode "q=SELECT count(value) FROM ' + tsname + '"'],0)
            if result.find("error") != -1:
                time.sleep(10)
                continue
            j = json.loads(result)
            return j['results'][0]['series'][0]['values'][0][1]
        return -1  #bogus count value to indicate error
#     def copyFile(self,node,filename):
#         shell = spur.SshShell(hostname=self.ip[node],username=self.username,private_key_file=self.pem)
#         with shell.open("/tmp","r") as remote_file
#             with open(".","w") as local_file
#                 shutil.copyfileobj(remote_file,local_file)
#         pass
    def printDebug(self):
        print self.ip[1],self.ip[2],self.ip[3],self.username,self.password,self.pem
