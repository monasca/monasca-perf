from testbase import TestBase
from influxparawrite import InfluxParaWrite

class test_308(TestBase):
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
        ipw.start(3,1,'killInflux',self.name)
        self.env.killInflux(2)
        self.env.killInflux(3)
        self.env.startInflux(1)
        self.env.startInflux(2)
        self.env.startInflux(3)
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
        return 'Kill node 1 while writing to 3. Kill node 2, then 3. Bring all nodes back up, 1 -> 3. Query.'
