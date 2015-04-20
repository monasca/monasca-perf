from testbase import TestBase
from influxparawrite import InfluxParaWrite

class test_212(TestBase):
    def run(self):
        self.env.sendSingleMetric(1,self.name,1)
        self.env.sendSingleMetric(2,self.name,2)
        self.env.sendSingleMetric(3,self.name,3)
        if self.env.countMetrics(1,self.name) != 3:
            return ["FAIL","node 1 wrong count"]
        if self.env.countMetrics(2,self.name) != 3:
            return ["FAIL","node 2 wrong count"]
        if self.env.countMetrics(3,self.name) != 3:
            return ["FAIL","node 3 wrong count"]
        self.env.partitionStart(3)
        ipw = InfluxParaWrite(self.env)
        ipw.start(1,3,'fullyPartitionNode',self.name)
        self.env.partitionStop(3)
        val = self.env.countMetrics(1,self.name)
        if val != ipw.count+3:
            return ["FAIL","node 1 wrong count 2: "+ str(val) + ' != '+str(ipw.count+3)]
        val = self.env.countMetrics(2,self.name)
        if val != ipw.count+3:
            return ["FAIL","node 2 wrong count 2: "+ str(val) + ' != '+str(ipw.count+3)]
        val = self.env.countMetrics(3,self.name)
        if val != ipw.count+3:
            return ["FAIL","node 3 wrong count 2: "+ str(val) + ' != '+str(ipw.count+3)]
        return ["PASS",""]
    def desc(self):
        return 'Partition away 3. Fire off multiple writes while partitioning the node. Remove the partition and query from that node'
