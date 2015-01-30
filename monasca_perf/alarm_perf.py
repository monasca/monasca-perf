import sys
import time
import multiprocessing

from monascaclient import client
from monascaclient import ksclient

import warnings
def no_warnings(message, category, filename, lineno):
    pass
warnings.showwarning = no_warnings

num_processes = 10
num_requests = 10
num_metrics = 10

# specify if the test should remove the generated alarms
cleanup_after_test = False

keystone = {
        'username':'mini-mon',
        'password':'password',
        'project':'test',
        'auth_url':'http://10.22.156.11:35357/v3'
    }

urls = [
    'https://mon-ae1test-monasca01.useast.hpcloud.net:8080/v2.0',
    'https://mon-ae1test-monasca02.useast.hpcloud.net:8080/v2.0',
    'https://mon-ae1test-monasca03.useast.hpcloud.net:8080/v2.0',
]

metric_name = 'alarm_perf'
metric_dimension = 'dim1'

alarm_def_name = 'alarm_perf_test'
alarm_def_expression = '{} > 0'.format(metric_name)

def cleanup(monasca_client, name):
    for definition in monasca_client.alarm_definitions.list():
        if definition['name'] == name:
            monasca_client.alarm_definitions.delete(**{'alarm_id':definition['id']})

def create_alarm_definition(monasca_client):
    try:
        resp = monasca_client.alarm_definitions.create(**{
            'name': alarm_def_name,
            'expression': alarm_def_expression,
            'match_by': [metric_dimension]
        })
        print('Alarm Definition ID: {}'.format(resp['id']))
        return resp['id']
    except Exception as ex:
        print('Could not create alarm definition\n{}'.format(ex))
        return None

def do_work(url_queue, proc_id, token):
    url = url_queue.get()
    monasca_client = client.Client('2_0', url, **{'token':token})
    try:
        for i in xrange(num_requests):
            start_num = proc_id*num_requests*num_metrics + i*num_metrics
            create_metrics(monasca_client, start_num, proc_id)
    except Exception as ex:
        print('Process {} exiting: {}'.format(proc_id, ex))

def create_metrics(monasca_client, start_num, id):
    try:
        body = []
        for i in xrange(start_num, start_num+num_metrics):
            body.append({
                'name': 'alarm_perf',
                'dimensions': {metric_dimension: 'value-{}'.format(i)},
                'value': 0,
                'timestamp': time.time()
            })
        monasca_client.metrics.create(**{'jsonbody':body})
    except Exception as ex:
        print('Process {} exiting: {}'.format(id, ex))

def alarm_performance_test():
    ks_client = None
    try:
        print('Authenticating with keystone on {}'.format(keystone['auth_url']))
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print('Failed to authenticate: {}'.format(ex))
        return False

    token = ks_client.token

    mon_client = client.Client('2_0', urls[0], **{'token':token})

    print('Removing old alarm definitions for {}'.format(alarm_def_name))
    cleanup(mon_client, alarm_def_name)

    print('Creating alarm definition')
    alarm_def_id = create_alarm_definition(mon_client)
    if not alarm_def_id:
        return False

    q = multiprocessing.Queue()
    for i in xrange(num_processes):
        url = urls[i % len(urls)]
        q.put(url.strip())

    print('Sending {} metrics'.format(num_processes*num_requests*num_metrics))
    start_time = time.time()

    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=do_work, args=(q, i, token))
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

    print('Sent in {} seconds'.format(final_time-start_time))

    if cleanup_after_test:
        cleanup(mon_client, alarm_def_name)

def main():
    if not alarm_performance_test():
        return 1

    return 0



if __name__ == "__main__":
    sys.exit(main())
