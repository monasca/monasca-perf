from testbase import TestBase

#This is the start of the partitioning tests (200-299)
class test_200(TestBase):
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
        self.env.fullyPartitionNode(3)
        self.env.sendSingleMetric(1,self.name,4)
        self.env.sendSingleMetric(2,self.name,5)
        if self.env.countMetrics(1,self.name) != 5:
            return ["FAIL","node 1 wrong count 2"]
        if self.env.countMetrics(2,self.name) != 5:
            return ["FAIL","node 2 wrong count 2"]
        self.env.partitionStop(3)
        self.env.sendSingleMetric(3,self.name,6)
        if self.env.countMetrics(1,self.name) != 6:
            return ["FAIL","node 1 wrong count 3"]
        if self.env.countMetrics(2,self.name) != 6:
            return ["FAIL","node 2 wrong count 3"]
        if self.env.countMetrics(3,self.name) != 6:
            return ["FAIL","node 3 wrong count 3"]        
        return ["PASS",""]
    def desc(self):
        return 'Fully partitions away node 3'
