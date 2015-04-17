from testbase import TestBase
from influxparawrite import InfluxParaWrite 

class test_104(TestBase):
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
        ipw = InfluxParaWrite(self.env)
        ipw.start(1,3,'stopInflux',self.name)
        self.env.startInflux(3)
        return ["PASS",""]
    def desc(self):
        return 'Shut down node 3. Fire off multiple writes while shutting down the node. Bring back up and query from that node'
