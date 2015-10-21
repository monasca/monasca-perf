import sys
import argparse

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

# monasca api urls
urls = [
    utils.env('MONASCA_API_URL'),
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('starttime', metavar='<UTC_START_TIME>',
                        help='metrics per second average >= UTC time. format: 2014-01-01T00:00:00Z.')
    parser.add_argument("--output_directory",
                        help="Output directory to place result files. Defaults to current directory", default='',
                        required=False)
    return parser.parse_args()


def query_metrics_per_second():

    args = parse_args()

    try:
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print 'Failed to authenticate: {}'.format(ex)
        return

    mon_client = client.Client('2_0', urls[0], token=ks_client.token)
    metrics_data = mon_client.metrics.list(name="metrics.published")

    all_metrics = []
    metric_averages = {}
    for metric in metrics_data:
        measurements = mon_client.metrics.list_measurements(start_time=args.starttime, name="metrics.published", dimensions=metric['dimensions'])
        values = []
        for m in measurements[0]['measurements']:
            values.append(m[1])
        metric_averages[metric['dimensions']['hostname']] = (sum(values)/len(values))
        all_metrics.append(values)
    if len(metric_averages) > 1:
        metric_averages['All Hosts'] = (sum(all_metrics)/len(all_metrics))
    with open(args.output_directory + 'metrics_per_second', "w") as output_file:
        for k, v in metric_averages.items():
            output_file.write("{} : {}\n".format(k, v))

if __name__ == "__main__":
    sys.exit(query_metrics_per_second())
