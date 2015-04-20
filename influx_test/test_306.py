from testbase import TestBase
from influxparawrite import InfluxParaWrite

class test_306(TestBase):
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
        self.env.killInflux(2)
        self.env.killInflux(3)
        self.env.startInflux(2)
        self.env.startInflux(3)
        ipw = InfluxParaWrite(self.env)
        ipw.start(1,2,'killInflux',self.name)
        self.env.startInflux(2)
        val = self.env.countMetrics(1,self.name)
        if val != 1003:
            return ["FAIL","node 1 wrong count 2: "+ str(val) + ' != 1003']
        val = self.env.countMetrics(2,self.name)
        if val != 1003:
            return ["FAIL","node 2 wrong count 2: "+ str(val) + ' != 1003']
        val = self.env.countMetrics(3,self.name)
        if val != 1003:
            return ["FAIL","node 3 wrong count 2: "+ str(val) + ' != 1003']
        return ["PASS",""]
    def desc(self):
        return 'Kill nodes 2 & 3. Bring back up node 2, then 3. Start writing to node 1 while bringing down node 2. Bring back up 2. Query'
