import influxenv

class TestBase(object):
    'Base for all of the test classes'
    def __init__(self,env,name):
        self.env = env
        self.name = name
    def run(self):
        pass
    def teardown(self):
        pass
    def name(self):
        return self.name
    def desc(self):
        return ''
