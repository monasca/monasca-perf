from testbase import TestBase
from influxparawrite import InfluxParaWrite 

class test_2(TestBase):
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
        ipw.start(1,3,'',self.name)
        if self.env.countMetrics(1,self.name) != 1003:
            return ["FAIL","node 1 wrong count 2"]
        if self.env.countMetrics(2,self.name) != 1003:
            return ["FAIL","node 2 wrong count 2"]
        if self.env.countMetrics(3,self.name) != 1003:
            return ["FAIL","node 3 wrong count 2"]
        return ["PASS",""]
    def desc(self):
        return 'Fire off multiple writes. Count the writes.'
