import sys
import argparse
from collections import defaultdict

from monascaclient import client
from monascaclient import ksclient
from monascaclient.common import utils

keystone = {
    'username': utils.env('OS_USERNAME'),
    'password': utils.env('OS_PASSWORD'),
    'token': utils.env('OS_AUTH_TOKEN'),
    'auth_url': utils.env('OS_AUTH_URL'),
    'service_type': utils.env('OS_SERVICE_TYPE'),
    'endpoint_type': utils.env('OS_ENDPOINT_TYPE'),
    'os_cacert': utils.env('OS_CACERT'),
    'user_domain_id': utils.env('OS_USER_DOMAIN_ID'),
    'user_domain_name': utils.env('OS_USER_DOMAIN_NAME'),
    'project_id': utils.env('OS_PROJECT_ID'),
    'project_name': utils.env('OS_PROJECT_NAME'),
    'domain_id': utils.env('OS_DOMAIN_ID'),
    'domain_name': utils.env('OS_DOMAIN_NAME'),
    'region_name': utils.env('OS_REGION_NAME')
}


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('starttime', metavar='<UTC_START_TIME>',
                        help='metrics per second average >= UTC time. format: 2014-01-01T00:00:00Z.')
    parser.add_argument('--endtime', metavar='<UTC_END_TIME>',
                        help='metrics per second average >= UTC time. format: 2014-01-01T00:00:00Z.',
                        required=False)
    parser.add_argument("--output_directory",
                        help="Output directory to place result files. Defaults to current directory", default='',
                        required=False)
    parser.add_argument("--monasca_api_url",
                        help="Monasca api url to use when querying. Example being http://192.168.10.4:8070/v2.0")
    return parser.parse_args()


def get_measurements_average(mon_client, start_time, metric_name, dimensions, end_time=None):

    if end_time is not None:
        measurements = mon_client.metrics.list_measurements(start_time=start_time, name=metric_name,
                                                            dimensions=dimensions,
                                                            end_time=end_time)
    else:
        measurements = mon_client.metrics.list_measurements(start_time=start_time, name=metric_name,
                                                            dimensions=dimensions)
    values = []
    for m in measurements[0]['measurements']:
        values.append(m[1])
    return sum(values) / len(values)


def query_measurement_average():

    args = parse_args()

    try:
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print 'Failed to authenticate: {}'.format(ex)
        return

    mon_client = client.Client('2_0', args.monasca_api_url, token=ks_client.token)

    metric_data = mon_client.metrics.list(name="process.cpu_perc")
    measurement_averages = defaultdict(dict)

    for metric in metric_data:
        dimensions = metric['dimensions']
        if dimensions['hostname'] not in measurement_averages:
            measurement_averages[dimensions['hostname']] = {}
        cpu_average = get_measurements_average(mon_client, args.starttime, "process.cpu_perc", dimensions, args.endtime)
        mem_average = get_measurements_average(mon_client, args.starttime, "process.mem.rss_mbytes", dimensions, args.endtime)
        measurement_averages[dimensions['hostname']][dimensions['process_name']] = [cpu_average, mem_average]
    for host in measurement_averages.keys():
        with open(args.output_directory + host, "w") as output_file:
            output_file.write("{:<30}| {:^10} | {:^10}\n".format("Process Name", "Cpu %", "Memory mb"))
            output_file.write("-------------------------------------------------------\n")
            for name in sorted(measurement_averages[host].keys()):
                output_file.write("{:<30}| {:>10.2f} | {:>10.2f}\n".format(name, measurement_averages[host][name][0],
                                                                           measurement_averages[host][name][1]))

if __name__ == "__main__":
    sys.exit(query_measurement_average())
