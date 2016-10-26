import datetime
import time
import os
import sys

from monascaclient import client
from monascaclient import ksclient

keystone = {
    'username': os.environ.get('OS_USERNAME', 'admin'),
    'password': os.environ.get('OS_PASSWORD', 'secretadmin'),
    'auth_url': os.environ.get('OS_AUTH_URL', 'http://127.0.0.1:35357/v3'),
    'project_name': os.environ.get('OS_PROJECT_NAME', 'admin')
}

monasca_url = "http://192.168.10.6:8070/v2.0"


def get_token(keystone):
    try:
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print 'Failed to authenticate: {}'.format(ex)
        return None
    return ks_client.token


def get_monasca_client(token):
    return client.Client('2_0', monasca_url, token=token)


def metric_list(args):
    token = get_token(keystone)
    mon_client = get_monasca_client(token)
    start_time = time.time()
    results = mon_client.metrics.list(**args)
    end_time = time.time()

    return {'start_time': start_time,
            'end_time': end_time,
            'data': results}


def measurement_list(args):
    token = get_token(keystone)
    mon_client = get_monasca_client(token)
    start_time = time.time()
    results = mon_client.metrics.list_measurements(**args)
    end_time = time.time()

    return {'start_time': start_time,
            'end_time': end_time,
            'data': results}


def statistics_list(args):
    token = get_token(keystone)
    mon_client = get_monasca_client(token)
    start_time = time.time()
    results = mon_client.metrics.list_statistics(**args)
    end_time = time.time()

    return {'start_time': start_time,
            'end_time': end_time,
            'data': results}


def print_results(results):
    if not isinstance(results['data'], list):
        print("Query failed")
        print(results['data'])
    elif not len(results['data']) > 0:
        print("Query failed")
        print("No results")
    else:
        print("Total results: {}".format(len(results['data'])))
        print("Total time: {}s".format(results['end_time'] - results['start_time']))


def run_queries():
    args = {}
    metric_list(args)

    metric_name = 'vm.mem.free_perc'
    resource_id = "12"
    device = "sda"

    print("No filters query")
    args = {}
    results = metric_list(args)
    print_results(results)

    print("\nMetric-list | Name only ")
    args = {'name': metric_name}
    results = metric_list(args)
    print_results(results)

    time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=300)
    print("\nMetric-list | Name and start time")
    args = {'name': metric_name,
            'start_time': time_stamp.isoformat()}
    results = metric_list(args)
    print_results(results)

    print("\nMetric-list | Name and max dimensions (single result)")
    args = {'name': metric_name,
            "dimensions": {
                "hostname": "test_vm_host_" + resource_id,
                "zone": "nova",
                "service": "compute",
                "cloud_name": "test_cloud",
                "resource_id": resource_id,
                "component": "vm",
                "cluster": "test_cluster"
            }}
    results = metric_list(args)
    print_results(results)

    print("\nMetric-list | Name and max dimensions (max results)")
    args = {'name': metric_name,
            'dimensions': {
                'zone': 'nova',
                'service': 'compute',
                'cloud_name': 'test_cloud',
                'component': 'vm',
                'cluster': 'test_cluster'
            }}
    results = metric_list(args)
    print_results(results)

    print("\nMetric-list | Dimensions only, resource_id (single vm results)")
    args = {'dimensions': {
        "resource_id": resource_id
    }}
    results = metric_list(args)
    print_results(results)

    print("\nMetric-list | Dimensions only, resource_id and device (single device result)")
    args = {'dimensions': {
        "resource_id": resource_id,
        "device": device
    }}
    results = metric_list(args)
    print_results(results)

    print("\nMetric-list | Dimensions only, device (multiple devices over multiple vms result)")
    args = {'dimensions': {
        "device": device
    }}
    results = metric_list(args)
    print_results(results)

    print("\nMeasurement-list | Name only merged (non-vm metric)")
    args = {'name': 'cpu.idle_perc',
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = measurement_list(args)
    print_results(results)

    print("\nMeasurement-list | Name only grouped (non-vm metric)")
    args = {'name': 'cpu.idle_perc',
            'start_time': '2016-01-01T00:00:00.000Z',
            'group_by': '*'}
    results = measurement_list(args)
    print_results(results)

    print("\nMeasurement-list | Name only merged (vm metric)")
    args = {'name': 'vm.mem.free_perc',
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = measurement_list(args)
    print_results(results)

    print("\nMeasurement-list | Name only grouped (vm metric)")
    args = {'name': 'vm.mem.free_perc',
            'start_time': '2016-01-01T00:00:00.000Z',
            'group_by': '*'}
    results = measurement_list(args)
    print_results(results)

    print("\nMeasurement-list | Name and resource_id (single result)")
    args = {'name': 'vm.mem.free_perc',
            'dimensions': {
                'resource_id': resource_id
            },
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = measurement_list(args)
    print_results(results)

    print("\nMeasurement-list | Name and max dimensions query (max results)")
    args = {'name': metric_name,
            'dimensions': {
                'zone': 'nova',
                'service': 'compute',
                'cloud_name': 'test_cloud',
                'component': 'vm',
                'cluster': 'test_cluster'
            },
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = measurement_list(args)
    print_results(results)

    print("\nMetric-statistics | name only merged (non-vm metric)")
    args = {'name': 'cpu.idle_perc',
            'statistics': 'max',
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = statistics_list(args)
    print_results(results)

    print("\nMetric-statistics | name only merged (vm metric)")
    args = {'name': 'vm.mem.free_perc',
            'statistics': 'max',
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = statistics_list(args)
    print_results(results)

    print("\nMetric-statistics | name and resource_id (single result)")
    args = {'name': 'vm.mem.free_perc',
            'dimensions': {
                'resource_id': resource_id
            },
            'statistics': 'max',
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = statistics_list(args)
    print_results(results)

    print("\nMetric-statistics | name and max dimensions query (max results)")
    args = {'name': metric_name,
            'dimensions': {
                'zone': 'nova',
                'service': 'compute',
                'cloud_name': 'test_cloud',
                'component': 'vm',
                'cluster': 'test_cluster'
            },
            'statistics': 'max',
            'start_time': '2016-01-01T00:00:00.000Z',
            'merge_metrics': 'true'}
    results = statistics_list(args)
    print_results(results)

if __name__ == "__main__":
    sys.exit(run_queries())
