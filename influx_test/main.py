import argparse
import influxenv
import importlib
import time
import glob
import os

def resetState():
    env.allPartitionStop()
    env.stopInflux(3)
    env.stopInflux(2)
    env.stopInflux(1)
    env.startInflux(1)
    env.startInflux(2)
    env.startInflux(3)


#
# Main Program
#
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ip1", help="node 1 ip")
    parser.add_argument("ip2", help="node 2 ip")
    parser.add_argument("ip3", help="node 3 ip")
    parser.add_argument("-u", "--username", help="username")
    parser.add_argument("-i", "--pemfile", help="pem file (path)")
    parser.add_argument("-t", "--test", help="run single test (number)")
    parser.add_argument("-r", "--range", help="run a range of tests (number:number)")

    args = parser.parse_args()

    env = influxenv.InfluxEnv(args.ip1,args.ip2,args.ip3,args.username,args.pemfile)
    #env.printDebug()
    
    resetState()
    env.createDB()
    num_pass = 0
    num_fail = 0
    
    test_list = []
    if args.test:
        test_list = ['test_'+args.test]
    else:
        test_list = [os.path.splitext(os.path.basename(x))[0] for x in glob.glob('test_*')]
    if args.range:
        test_range = args.range.split(":")
        test_list2 = []
        for x in test_list:
            value = int(x.split("_")[1])
            if value >= int(test_range[0]) and value <= int(test_range[1]):
                test_list2.append(x)
        test_list = test_list2
    for test_name in test_list:
        epoch_time = int(time.time())
        env.testLogBanner(test_name)
        resetState()
        
        test_module = importlib.import_module(test_name)
        test = getattr(test_module,test_name)(env,test_name)
        test_result = test.run()
        test.teardown()
        test_time = int(time.time()) - epoch_time
        if test_result[0] == 'PASS':
            print test_result[0] + ": " + test.name + " : " + test.desc() + " (" + str(test_time) + "s)"
            num_pass += 1
        else:
            print test_result[0] + " (" + test_result[1] +")" + ": " + test.name + " : " + test.desc() + " (" + str(test_time) + "s)"
            num_fail += 1
        
    resetState()

    print "Number of PASS:", num_pass
    print "Number of FAIL:", num_fail
    
    





