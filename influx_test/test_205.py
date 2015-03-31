from testbase import TestBase

class test_205(TestBase):
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
        self.env.partitionStart(3)
        self.env.fullPartitionNode(2)
        self.env.fullPartitionNode(3)
        #These writes should all fail
        self.env.sendSingleMetric(1,self.name,4)
        self.env.sendSingleMetric(2,self.name,5)
        self.env.sendSingleMetric(3,self.name,6)
        self.env.partitionStop(2)
        self.env.partitionStop(3)
        self.env.sendSingleMetric(1,self.name,7)
        self.env.sendSingleMetric(2,self.name,8)
        self.env.sendSingleMetric(3,self.name,9)
        if self.env.countMetrics(1,self.name) != 6:
            return ["FAIL","node 1 wrong count 2"]
        if self.env.countMetrics(2,self.name) != 6:
            return ["FAIL","node 2 wrong count 2"]
        if self.env.countMetrics(3,self.name) != 6:
            return ["FAIL","node 3 wrong count 2"]        
        return ["PASS",""]
    def desc(self):
        return 'Partition all nodes from each other'
