from testbase import TestBase

class test_207(TestBase):
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
        self.env.partitionStart(2)
        self.env.unidirectionalSinglePartitionNode(2,3)
        self.env.sendSingleMetric(3,self.name,4)
        self.env.sendSingleMetric(1,self.name,5)
        if self.env.countMetrics(3,self.name) != 5:
            return ["FAIL","node 3 wrong count 2"]
        if self.env.countMetrics(1,self.name) != 5:
            return ["FAIL","node 1 wrong count 2"]
        self.env.partitionStop(2)
        self.env.sendSingleMetric(2,self.name,6)
        if self.env.countMetrics(1,self.name) != 6:
            return ["FAIL","node 1 wrong count 3"]
        if self.env.countMetrics(2,self.name) != 6:
            return ["FAIL","node 2 wrong count 3"]
        if self.env.countMetrics(3,self.name) != 6:
            return ["FAIL","node 3 wrong count 3"]        
        return ["PASS",""]
    def desc(self):
        return 'Unidirectionally partition node 2 away from node 3'
