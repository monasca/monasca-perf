# ADG 3/2015 - customized for test cluster baselline tests
# ADG 3/2015 - modified to work with json object versus array response

import datetime
import re
import sys
import time
import multiprocessing

import _mysql

from monascaclient import client
from monascaclient import ksclient

import warnings

# suppress warnings to improve performance
def no_warnings(message, category, filename, lineno):
    pass
warnings.showwarning = no_warnings

num_definitions = 4
num_processes = 10
num_requests = 10
num_metrics = 10

max_wait_time = 200  # Seconds

total_metrics = num_processes*num_requests*num_metrics

# specify if the test should remove the generated alarms
cleanup_after_test = False

keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    'auth_url': 'http://10.22.156.20:5001/v3'
}

# monasca api urls
urls = [
    'https://mon-ae1test-monasca01.useast.hpcloud.net:8080/v2.0',
    'https://mon-ae1test-monasca02.useast.hpcloud.net:8080/v2.0',
    'https://mon-ae1test-monasca03.useast.hpcloud.net:8080/v2.0',
]

mysql_cfg = {
    'host':'10.22.156.11',
    'user': 'monapi',
    'passwd': 'password',
    'db': 'mon'
}

metric_name = 'alarm_perf'
metric_dimension = 'dim1'

alarm_def_name = 'alarm_perf_test'
alarm_def_expression = '{} > 0'

def cleanup(monasca_client, name):
    matched = 0
    pattern = re.compile(metric_name+'[0-9]*')
    for definition in monasca_client.alarm_definitions.list()['elements']:
        if pattern.match(definition['name']):
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


def do_work(url, sent_queue, proc_id, token):
    monasca_client = client.Client('2_0', url, token=token)
    start_num = (proc_id * num_requests * num_metrics)
    sent = 0
    try:
        for i in xrange(num_requests):
            try:
                create_metrics(monasca_client, proc_id, start_num + (i*num_metrics))
                sent += num_metrics
            except Exception:
                pass
    except Exception as ex:
        sent_queue.put('Process {} exited: {}'.format(proc_id, ex))
        sent_queue.put(sent)
    sent_queue.put(sent)


def create_metrics(monasca_client, id, start_number):
    body = []
    for i in xrange(num_metrics):
        body.append({
            'name': metric_name+str(id%num_definitions),
            'dimensions': {metric_dimension: 'value-{}'.format(start_number+i), 'hostname': 'node' + str(i)},
            'value': 0,
            'timestamp': time.time()*1000
        })
    monasca_client.metrics.create(jsonbody=body)


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
    try:
        print('Authenticating with keystone on {}'.format(keystone['auth_url']))
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print('Failed to authenticate: {}'.format(ex))
        return False

    token = ks_client.token

    mon_client = client.Client('2_0', urls[0], token=token)

    print('Removing old alarm definitions for {}'.format(alarm_def_name))
    cleanup(mon_client, alarm_def_name)

    # pause to allow thresh to complete internal cleanup of old alarms
    time.sleep(10)

    print('Creating alarm definitions')
    for i in range(0,num_definitions):
        expression = alarm_def_expression.format(metric_name+str(i))
        alarm_def_id = create_alarm_definition(mon_client, alarm_def_name+str(i), expression)
        if not alarm_def_id:
            return False

    sent_q = multiprocessing.Queue()

    print('Sending {} metrics'.format(total_metrics))
    start_datetime = datetime.datetime.now()
    start_datetime = start_datetime - datetime.timedelta(microseconds=start_datetime.microsecond)
    start_datetime.strftime("%y-%m-%d %H:%M:%S")
    start_time = time.time()

    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=do_work, args=(urls[i % len(urls)],
                                                          sent_q, i, token))
        process_list.append(p)
        p.start()

    try:
        for p in process_list:
            try:
                p.join()
            except Exception:
                pass

    except KeyboardInterrupt:
        return False

    final_time = time.time()

    # There is some chance that not all metrics were sent (lost connection, bad status, etc.)
    total_metrics_sent = aggregate_sent_metric_count(sent_q)
    print('Sent {} in {} seconds'.format(total_metrics_sent,final_time-start_time))

    print('Waiting for alarms to reach mySQL database')
    alarm_count = 0
    delta = 0
    last_change = time.time()
    conn = _mysql.connect(**mysql_cfg)
    while alarm_count < total_metrics_sent:
        conn.query("select count(*) from alarm where created_at >= \"{}\";".format(start_datetime))
        result = conn.store_result()
        alarm_count = int(result.fetch_row()[0][0])
        delta = time.time() - start_time
        if (last_change + max_wait_time) <= time.time():
            print("Max wait time exceeded, {} alarms found".format(alarm_count))
            return False
        time.sleep(1)

    conn.close()
    print("-----Test Results-----")
    print("{} alarms in {} seconds".format(alarm_count, delta))
    print("{} per second".format(alarm_count/delta))

    if cleanup_after_test:
        cleanup(mon_client, alarm_def_name)
    return True


def main():
    if not alarm_performance_test():
        print("-----Test failed to complete-----")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
