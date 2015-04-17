import datetime
import re
import sys
import time
import multiprocessing

from monascaclient import client
from monascaclient import ksclient

from agent_sim import agent_sim_process

import warnings

# suppress warnings to improve performance
def no_warnings(message, category, filename, lineno):
    pass
warnings.showwarning = no_warnings

num_processes = 10
num_requests = 4
num_metrics = 100
num_definitions = 2

max_wait_time = 20  # Seconds

# specify if the test should remove the generated alarms
cleanup_after_test = False

keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    #'auth_url': 'http://10.22.156.20:35358/v3',
    'auth_url': 'http://192.168.10.5:35357/v3'
}

# monasca api urls
urls = [
    #'https://mon-ae1test-monasca01.useast.hpcloud.net:8080/v2.0',
    #'https://mon-ae1test-monasca02.useast.hpcloud.net:8080/v2.0',
    #'https://mon-ae1test-monasca03.useast.hpcloud.net:8080/v2.0',
    'http://192.168.10.4:8080/v2.0',
]

metric_name = 'alarm_perf'
metric_dimension = 'dim1'

alarm_def_name = 'alarm_perf_test'
alarm_def_expression = '{} > 0'

if len(sys.argv) >= 2:
    num_processes = int(sys.argv[1])

total_metrics = num_processes*num_requests*num_metrics
pattern = re.compile(alarm_def_name+'[0-9]+')

class MetricCreatorAlarmPerf():
    """ Generates metrics
    """
    def __init__(self, proc_num):
        self.proc_num = proc_num
        self.num_calls = 0

    def create_metric(self):
        host_num = self.num_calls + self.proc_num * num_requests * num_metrics
        metric = {"name": metric_name + str(self.proc_num % num_definitions),
                  "dimensions": {metric_dimension: "value-" + str(host_num)},
                  "timestamp": time.time()*1000 + self.num_calls, # make sure each timestamp is unique,
                                                                  # else influx 9 will overwrite previous metric
                  "value": 0}
        self.num_calls += 1
        return metric

def cleanup(monasca_client, name):
    matched = 0
    for definition in monasca_client.alarm_definitions.list():
        if pattern.match(definition['name']):
            print(definition['name'])
            monasca_client.alarm_definitions.delete(alarm_id=definition['id'])
            matched += 1
    print("Removed {} definitions".format(matched))


def create_alarm_definition(monasca_client, name, expression):
    try:
        resp = monasca_client.alarm_definitions.create(
            name=name,
            expression=expression,
            match_by=[metric_dimension]
        )
        print('Alarm Definition ID: {}'.format(resp['id']))
        return resp['id']
    except Exception as ex:
        print('Could not create alarm definition\n{}'.format(ex))
        return None


def aggregate_sent_metric_count(sent_q):
    total_sent = 0
    while not sent_q.empty():
        item = sent_q.get()
        if isinstance(item,int):
            total_sent += item
        else:
            print(item)
    return total_sent


def alarm_performance_test():
    if num_processes < num_definitions:
        return False, "Number of agents ({0}) must be >= number of definitions ({1})".format(num_processes,
                                                                                             num_definitions)

    try:
        print('Authenticating with keystone on {}'.format(keystone['auth_url']))
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        return False, 'Failed to authenticate: {}'.format(ex)

    mon_client = client.Client('2_0', urls[0], token=ks_client.token)

    print('Removing old alarm definitions for {}'.format(alarm_def_name))
    cleanup(mon_client, alarm_def_name)

    alarm_def_id_list = []
    print('Creating alarm definitions')
    for i in xrange(num_definitions):
        expression = alarm_def_expression.format(metric_name+str(i))
        alarm_def_id = create_alarm_definition(mon_client, alarm_def_name+str(i), expression)
        if not alarm_def_id:
            return False, "Failed to create alarm definition"
        alarm_def_id_list.append(alarm_def_id)

    sent_q = multiprocessing.Queue()

    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=agent_sim_process(i, num_requests, num_metrics, urls[(i % len(urls))],
                                                             keystone, queue=sent_q,
                                                             metric_creator=MetricCreatorAlarmPerf).run)
        process_list.append(p)

    start_datetime = datetime.datetime.now()
    start_datetime = start_datetime - datetime.timedelta(microseconds=start_datetime.microsecond)
    print("Starting test at: " + start_datetime.isoformat())
    start_time = time.time()

    for p in process_list:
        p.start()

    try:
        for p in process_list:
            try:
                p.join()
            except Exception:
                pass

    except KeyboardInterrupt:
        return False, "User interrupt"

    final_time = time.time()

    # There is some chance that not all metrics were sent (lost connection, bad status, etc.)
    total_metrics_sent = aggregate_sent_metric_count(sent_q)
    print('Sent {} metrics in {} seconds'.format(total_metrics_sent,final_time-start_time))
    if total_metrics_sent <= 0:
        return False, "Failed to send metrics"

    print('Waiting for alarms to be created')
    alarm_count = 0
    last_count = 0
    last_change = time.time()
    while alarm_count < total_metrics_sent:
        alarm_count = 0
        for id in alarm_def_id_list:
            num = len(mon_client.alarms.list(alarm_definition_id=id))
            alarm_count += num
        if alarm_count > last_count:
            last_change = time.time()
            last_count = alarm_count

        if (last_change + max_wait_time) <= time.time():
            metrics_found = 0
            for i in xrange(num_definitions):
                val = len(mon_client.metrics.list_measurements(start_time=start_datetime.isoformat(), name=metric_name+str(i),
                                                       merge_metrics=True)[0]['measurements'])
                metrics_found += val
            return False, "Max wait time exceeded, {0} / {1} alarms found".format(alarm_count, metrics_found)
        time.sleep(1)

    delta = last_change - start_time

    tot_met = 0
    for i in xrange(num_definitions):
        metrics = mon_client.metrics.list_measurements(start_time=start_datetime.isoformat(), name=metric_name+str(i),
                                                       merge_metrics=True)
        tot_met += len(metrics[0]['measurements'])

    print("Metrics from api: {}".format(tot_met))
    print("-----Test Results-----")
    print("{} alarms in {} seconds".format(alarm_count, delta))
    print("{} per second".format(alarm_count/delta))

    if cleanup_after_test:
        cleanup(mon_client, alarm_def_name)
    return True, ""


def main():
    success, msg = alarm_performance_test()
    if not success:
        print("-----Test failed to complete-----")
        print(msg)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
