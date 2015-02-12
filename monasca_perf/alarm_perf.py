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

num_processes = 10
num_requests = 1
num_metrics = 10

total_metrics = num_processes*num_requests*num_metrics

# specify if the test should remove the generated alarms
cleanup_after_test = False

keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    #'auth_url': 'http://10.22.156.11:35357/v3',
    'auth_url': 'http://192.168.10.5:35357/v3'
}

# monasca api urls
urls = [
    #'https://mon-ae1test-monasca01.useast.hpcloud.net:8080/v2.0',
    #'https://mon-ae1test-monasca02.useast.hpcloud.net:8080/v2.0',
    #'https://mon-ae1test-monasca03.useast.hpcloud.net:8080/v2.0',
    'http://192.168.10.4:8080/v2.0',
]

mysql_cfg = {
    #'host':'10.22.156.11',
    'host': '192.168.10.4',
    'user': 'monapi',
    'passwd': 'password',
    'db': 'mon'
}
# interval between prints of current values
mysql_report_interval = 15  # Seconds

metric_name = 'alarm_perf'
metric_dimension = 'dim1'

alarm_def_name = 'alarm_perf_test'
alarm_def_expression = '{} > 0'.format(metric_name)

def cleanup(monasca_client, name):
    for definition in monasca_client.alarm_definitions.list():
        if definition['name'] == name:
            monasca_client.alarm_definitions.delete(alarm_id=definition['id'])


def create_alarm_definition(monasca_client):
    try:
        resp = monasca_client.alarm_definitions.create(
            name=alarm_def_name,
            expression=alarm_def_expression,
            match_by=[metric_dimension]
        )
        print('Alarm Definition ID: {}'.format(resp['id']))
        return resp['id']
    except Exception as ex:
        print('Could not create alarm definition\n{}'.format(ex))
        return None


def do_work(url_queue, sent_queue, proc_id, token):
    url = url_queue.get()
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
            'name': metric_name,
            'dimensions': {metric_dimension: 'value-{}'.format(start_number+i)},
            'value': 0,
            'timestamp': time.time()
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

    print('Creating alarm definition')
    alarm_def_id = create_alarm_definition(mon_client)
    if not alarm_def_id:
        return False

    url_q = multiprocessing.Queue()
    for i in xrange(num_processes):
        url = urls[i % len(urls)]
        url_q.put(url.strip())

    sent_q = multiprocessing.Queue()

    print('Sending {} metrics'.format(total_metrics))
    start_time = time.time()

    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=do_work, args=(url_q, sent_q, i, token))
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
    alarms = 0
    delta = 0
    last_two = [-1,-1]
    conn = _mysql.connect(**mysql_cfg)
    while alarms < total_metrics_sent:
        conn.query("select count(*) from alarm where alarm_definition_id=\"{}\";".format(alarm_def_id))
        result = conn.store_result()
        alarms = int(result.fetch_row()[0][0])
        delta = time.time() - start_time
        if int(delta) % mysql_report_interval == 0:
            print("{} of {} alarms in {} seconds".format(alarms, total_metrics_sent, delta))
            if alarms == last_two[0] and last_two[0] == last_two[1]:
                print("No change over {} seconds.".format(mysql_report_interval*2))
                return False
            else:
                last_two[1] = last_two[0]
                last_two[0] = alarms
        time.sleep(1)

    conn.close()
    print("-----Test Results-----")
    print("{} alarms in {} seconds".format(alarms, delta))
    print("{} per second".format(alarms/delta))

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
