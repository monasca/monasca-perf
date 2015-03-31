from testbase import TestBase

#This is the start of the killed influx tests (300-399)
class test_300(TestBase):
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
        self.env.killInflux(3)
        self.env.sendSingleMetric(1,self.name,4)
        self.env.sendSingleMetric(2,self.name,5)
        if self.env.countMetrics(1,self.name) != 5:
            return ["FAIL","node 1 wrong count 2"]
        if self.env.countMetrics(2,self.name) != 5:
            return ["FAIL","node 2 wrong count 2"]
        self.env.startInflux(3)
        if self.env.countMetrics(3,self.name) != 5:
            return ["FAIL","node 3 wrong count 2"]
        self.env.sendSingleMetric(3,self.name,6)
        if self.env.countMetrics(1,self.name) != 6:
            return ["FAIL","node 1 wrong count 3"]
        if self.env.countMetrics(2,self.name) != 6:
            return ["FAIL","node 2 wrong count 3"]
        if self.env.countMetrics(3,self.name) != 6:
            return ["FAIL","node 3 wrong count 3"]      
        return ["PASS",""]
    def desc(self):
        return 'Kill node 3. Fire off several different writes. Bring back up and query from that node'
