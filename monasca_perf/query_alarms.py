import sys
import time
import multiprocessing
import random
import argparse

from monascaclient.common import utils
from monascaclient import client
from monascaclient import ksclient

max_wait_time = 20
min_wait_time = 5

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
    parser.add_argument("--number_processes", help="Number of processes to run against the API", type=int,
                        required=False, default=10)
    parser.add_argument("--monasca_api_url",
                        help="Monasca api url to use when querying. Example being http://192.168.10.4:8070/v2.0")
    return parser.parse_args()


def query_alarms(monasca_api_url):
    try:
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print 'Failed to authenticate: {}'.format(ex)
        return

    mon_client = client.Client('2_0', monasca_api_url, token=ks_client.token)
    while True:
        try:
            time.sleep(random.randint(min_wait_time, max_wait_time))
            ## TO DO WRITE THE AMOUNT OF TIME TO GET ANSWER BACK TO FILE AND AVERAGE AT THE END
            alarms = mon_client.alarms.list()
        except KeyboardInterrupt:
            return


def query_alarms_test():

    args = parse_args()
    num_processes = args.number_processes

    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=query_alarms, args=(args.monasca_api_url,))
        process_list.append(p)

    for p in process_list:
        p.start()

    try:
        for p in process_list:
            try:
                p.join()
            except Exception:
                pass

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    sys.exit(query_alarms_test())
