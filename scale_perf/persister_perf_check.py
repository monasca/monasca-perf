from __future__ import print_function
__author__ = 'craigbr'

import os
import subprocess
import sys
import time

COMMAND = '/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zookeeper zookeeper:2181 --topic metrics --group 1_metrics'
args = COMMAND.split(' ')

def get_num_metrics_left():
    '''Call kafka to determine the total lag and logsize of the persister'''

    try:
        stdout = subprocess.check_output(args, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    lines = stdout.split('\n')
    total_lag = 0
    total_measurements = 0
    for line in lines:
        # Group           Topic                          Pid Offset          logSize         Lag             Owner
        # 1_metrics       metrics                        0   60              60              0               none
        if '1_metrics' in line:
            fields = line.split()
            total_lag += int(fields[5])
            total_measurements += int(fields[4])
    return total_lag, total_measurements

# Kafka is in different places in container vs devstack
if not os.path.exists(args[0]):
    args[0] = '/opt' + args[0]
    if not os.path.exists(args[0]):
        print('kafka-run-class.sh does not exist in /kafka/bin or /opt/kafka/bin', file=sys.stderr)
        sys.exit()

#
# Run the agent simulator and see how long it takes the persister to process all of the metrics.
# Since the persister may take up to 15 seconds to flush the last batch, this is not the true
# speed, but is close enough for comparisons.
# There should be no agents running, that will make the test less accurate
#
n, starting_measurements = get_num_metrics_left()
if n != 0:
    print('''Persister is already lagging {} metrics, can't run test. Ensure agents are OFF!'''.format(n),
          file=sys.stderr)
    sys.exit(1)

start_time = time.time()

start_test = '''python agent_simulator.py --number_agents 1 --run_time 1 --no_wait'''

print('Starting agent_simulator')
test = subprocess.Popen(start_test.split())
print('agent_simulator running')

n, _ = get_num_metrics_left()
while n == 0:
    n, _ = get_num_metrics_left()

n, ending_measurements = get_num_metrics_left()
while n != 0:
    n, ending_measurements = get_num_metrics_left()

end_time = time.time()

rc = test.wait()
print('Return code from agent_simulator is {}'.format(rc))

num_measurements = ending_measurements - starting_measurements
total_time = end_time - start_time
rate = num_measurements / total_time
print('''Read {} measurements in {} seconds. Rate = {} measurements/second'''.format(num_measurements, total_time, rate))
