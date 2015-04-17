from multiprocessing import Process
import time
import sys

count = 1000 #Note!!!!!: This needs to be a multiple of env.numThreads, else tests will likely fail with the wrong count(s)
actionWaitTime = 5

class InfluxParaWrite(object):
    def __init__(self,env):
        self.env = env
        self.count = count
        if self.count % self.env.numThreads != 0:
            print "influxparawrite.count is not a multiple of env.numThreads"
            sys.exit(1)
        self.actionWaitTime = actionWaitTime
    def start(self,write_node,action_node,action,tsname):
        self.write_node = write_node
        self.action_node = action_node
        self.action = action
        self.tsname = tsname
        p_write = Process(target=self.doWrites,args=(write_node,tsname))
        p_write.start()
        p_action = Process(target=self.doAction, args=(action_node,action))
        p_action.start()
        p_action.join()
        p_write.join()
    def doAction(self,node,action):
        if len(action) == 0: return
        time.sleep(self.actionWaitTime)
        method = getattr(self.env,action)
        if not method:
            raise Exception("Method %s not implemented" % action)
        method(node)
    def doWrites(self,node,tsname):
        self.env.sendMultipleMetrics(node,tsname,self.count)
