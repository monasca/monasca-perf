import os
import random
import socket
import sys
import time
import multiprocessing
import argparse
import uuid

import yaml


import metric_simulator

from monascaclient import client
from monascaclient import ksclient
from monascaclient import exc

wait_time = 30

no_wait = False

number_of_agents = int(os.environ.get('NUM_AGENTS', '0'))
number_of_containers = int(os.environ.get('NUM_CONTAINERS', '10'))
number_of_metrics = int(os.environ.get('NUM_METRICS', '1310'))


class AgentInfo:

    def __init__(self):
        pass

    keystone = {}
    monasca_url = ''


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--number_agents", help="Number of agents to emulate sending metrics to the API", type=int,
                        required=False, default=30)
    parser.add_argument("--run_time",
                        help="How long, in mins, collection will run. Defaults to run indefinitely until the user hits"
                             " control c", required=False, type=int, default=None)
    parser.add_argument("--no_wait", help="Send measurements as fast as possible", action="store_true")
    return parser.parse_args()


def get_token(keystone):
    try:
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print 'Failed to authenticate: {}'.format(ex)
        return None
    return ks_client.token


def create_metric_list(process_number, container_names):
    metrics = []
    for i in xrange(number_of_metrics):
        epoch = int(time.time()) - 120
        metrics.append({"name": "perf-parallel-" + str(i),
                        "dimensions": {"perf-id": str(process_number),
                                       "zone": "nova",
                                       "service": "compute",
                                       "resource_id": "34c0ce14-9ce4-4d3d-84a4-172e1ddb26c4",
                                       "tenant_id": "71fea2331bae4d98bb08df071169806d",
                                       "hostname": socket.gethostname(),
                                       "component": "vm",
                                       "control_plane": "ccp",
                                       "cluster": "compute",
                                       "cloud_name": "monasca",
                                       "container": container_names[i % len(container_names)]},
                        "timestamp": epoch * 1000,
                        "value": i})

    # can make it an argument
    percentage_of_known_metrics = 10
    known_metric_generator = metric_simulator.generate_metrics()

    # insert known metrics randomly into dummy metrics.
    # known_metric_generator can generate known_metrics indefinitely
    for _ in xrange(number_of_metrics * percentage_of_known_metrics /100):
        insert_position = random.randint(0,number_of_metrics-1)
        known_metric = known_metric_generator.next()
        metrics.insert(insert_position, known_metric)

    return metrics


def send_metrics(agent_info, process_number):
    container_names = [uuid.uuid4().hex for i in range(number_of_containers)]

    if not no_wait:
        time.sleep(random.randint(0, 60))
    token = get_token(agent_info.keystone)
    if token is None:
        return
    while True:
        try:
            mon_client = client.Client('2_0', agent_info.monasca_url, token=token)
            start_send = time.time()
            metrics = create_metric_list(process_number, container_names)
            mon_client.metrics.create(jsonbody=metrics)
            end_send = time.time()
            secs = end_send - start_send
            if not no_wait:
                time.sleep(wait_time-secs)
        except KeyboardInterrupt:
            return
        except exc.HTTPUnauthorized:
            token = get_token(agent_info.keystone)


def parse_agent_config(agent_info):
    agent_config_file = open('/etc/monasca/agent/agent.yaml')
    agent_config = yaml.load(agent_config_file)
    agent_info.keystone['username'] = agent_config['Api']['username']
    agent_info.keystone['password'] = agent_config['Api']['password']
    agent_info.keystone['auth_url'] = agent_config['Api']['keystone_url']
    agent_info.keystone['project_name'] = agent_config['Api']['project_name']
    agent_info.monasca_url = agent_config['Api']['url']


def agent_simulator_test():
    global no_wait
    args = parse_args()
    no_wait = args.no_wait
    num_processes = number_of_agents or args.number_agents
    agent_info = AgentInfo()
    parse_agent_config(agent_info)
    process_list = []
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=send_metrics, args=(agent_info, i))
        process_list.append(p)

    for p in process_list:
        p.start()
    if args.run_time is not None:
        time.sleep(args.run_time * 60)
        for p in process_list:
            p.terminate()
    else:
        try:
            for p in process_list:
                try:
                    p.join()
                except Exception:
                    pass
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    sys.exit(agent_simulator_test())
