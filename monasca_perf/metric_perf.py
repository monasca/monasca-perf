import datetime
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

max_wait_time = 20  # Seconds

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

if len(sys.argv) >= 2:
    num_processes = int(sys.argv[1])

total_metrics = num_processes*num_requests*num_metrics

class MetricCreatorMetricPerf():
    """ Generates metrics
    """
    def __init__(self, proc_num):
        self.proc_num = proc_num
        self.num_calls = 0

    def create_metric(self):
        metric = {"name": "metric_perf" + str(self.num_calls % num_metrics),
                  "dimensions": {"dim1": "value-" + str(self.proc_num)},
                  "timestamp": time.time()*1000 + self.num_calls, # make sure each timestamp is unique,
                                                                  # else influx 9 will overwrite previous metric
                  "value": 0}
        self.num_calls += 1
        return metric


def aggregate_sent_metric_count(sent_q):
    total_sent = 0
    while not sent_q.empty():
        item = sent_q.get()
        if isinstance(item,int):
            total_sent += item
        else:
            print(item)
    return total_sent


def metric_performance_test():

    try:
        print('Authenticating with keystone on {}'.format(keystone['auth_url']))
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        return False, 'Failed to authenticate: {}'.format(ex)

    mon_client = client.Client('2_0', urls[0], token=ks_client.token)

    sent_q = multiprocessing.Queue()

    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=agent_sim_process(i, num_requests, num_metrics, urls[(i % len(urls))],
                                                             keystone, queue=sent_q,
                                                             metric_creator=MetricCreatorMetricPerf).run)
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



    total_metrics_sent = aggregate_sent_metric_count(sent_q)

    metrics_found = 0
    last_count = 0
    last_change = time.time()
    while metrics_found < total_metrics_sent:

        metrics_found = 0
        for i in xrange(num_metrics):
            try:
                val = len(mon_client.metrics.list_measurements(start_time=start_datetime.isoformat(),
                                                               name="metric_perf"+str(i),
                                                               merge_metrics=True)[0]['measurements'])
                metrics_found += val
            except Exception as ex:
                print("Failed to retrieve metrics from api\n{}".format(ex))

        if metrics_found > last_count:
            last_change = time.time()
            last_count = metrics_found

        if (last_change + max_wait_time) <= time.time():
            return False, "Max wait time exceeded, {0} / {1} metrics found".format(metrics_found, total_metrics_sent)
        time.sleep(1)

    final_time = time.time()
    print("-----Test Results-----")
    print("{} metrics in {} seconds".format(metrics_found, final_time-start_time))
    print("{} per second".format(metrics_found / (final_time - start_time)))

    return True, ""


def main():
    success, msg = metric_performance_test()
    if not success:
        print("-----Test failed to complete-----")
        print(msg)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
