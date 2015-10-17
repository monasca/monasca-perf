# (C) Copyright 2015 Hewlett Packard Enterprise Development Company LP
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import random
import time

from monascaclient import ksclient as monasca_ksclient
from monascaclient import client as monasca_client

import numpy as np

AUTH_URL = 'http://192.168.10.5:35357/v3'
USERNAME = 'mini-mon'
PASSWORD = 'password'
DOMAIN_NAME = 'Default'
PROJECT_NAME = 'mini-mon'

NUM_METRICS = 100
METRIC_NAME = 'process_status'
METRIC_LAG_TIME_SECS = 30

ALARM_DEFINITION_NAME = 'process_status'
ALARM_EXPRESSION = 'max(process_status{}, 60) > 0'

THRESHOLD_ENGINE_EVALUATION_PERIOD = 60

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('monasca-perf')
logging.getLogger("requests").setLevel(logging.WARNING)

def create_metric(id, value):
    metric = {}
    metric['name'] = METRIC_NAME
    metric['value'] = value
    metric['timestamp'] = (time.time() - METRIC_LAG_TIME_SECS)*1000
    metric['dimensions'] = {'id': id}
    return metric

def send_metrics(client, metrics):
    kwargs = {}
    kwargs['jsonbody'] = metrics
    client.metrics.create(**kwargs)

def create_alarm_definition():
    defn = {}
    defn['name'] = ALARM_DEFINITION_NAME
    defn['expression'] = ALARM_EXPRESSION
    defn['match_by'] = 'id'
    return defn

def send_alarm_definition(client, defn):
    try:
        client.alarm_definitions.create(**defn)
    except Exception as e:
        # Alarm definition already exists so continue
        logger.warn('Alarm definition already exists')
        pass

def get_alarms(client):
    kwargs = {}
    kwargs['name'] = ALARM_DEFINITION_NAME
    response = client.alarms.list(**kwargs)
    return response

def wait_for_all_alarms_to_transition(client, state):
    all_transitioned = False
    while not all_transitioned:
        metrics = [create_metric(str(id), 0.0)
                   for id in xrange(NUM_METRICS)]
        send_metrics(client, metrics)
        alarms = get_alarms(client)
        transitioned = map(lambda x: x['state'] == state, alarms)
        all_transitioned = reduce(lambda x, y: x & y, transitioned)
        time.sleep(1)

keystone_client = monasca_ksclient.KSClient(auth_url=AUTH_URL,
                                            username=USERNAME,
                                            project_name=PROJECT_NAME,
                                            domain_name=DOMAIN_NAME,
                                            password=PASSWORD)
monasca_client = monasca_client.Client('2_0',
                                       keystone_client.monasca_url,
                                       token=keystone_client.token)

# Create alarm definition and wait 10 secsond for alarms to be created
logger.info('create alarm definition and wait 10 seconds for alarms to be created')
alarm_defn = create_alarm_definition()
send_alarm_definition(monasca_client, alarm_defn)
metrics = [create_metric(str(id), 0.0) for id in xrange(NUM_METRICS)]
send_metrics(monasca_client, metrics)
time.sleep(10)
logger.info('done')

# Wait for all alarms to transition to OK
logger.info('wait for all alarms to transition to OK')
wait_for_all_alarms_to_transition(monasca_client, 'OK')
logger.info('done')

# Wait for 120 seconds + a random time in the THRESHOLD_ENGINE_EVALUATION_PERIOD
logger.info('wait for 120 seconds + a random time between 0 and 59 seconds')
start_time = time.time()
wait_time = 120 + random.randint(0, THRESHOLD_ENGINE_EVALUATION_PERIOD-1)
while True:
    metrics = [create_metric(str(id), 0.0) for id in xrange(NUM_METRICS)]
    send_metrics(monasca_client, metrics)
    end_time = time.time()
    if end_time > start_time + wait_time:
        break
    time.sleep(1)
logger.info('done')

# Create random transition times for metrics in the THRESHOLD_ENGINE_EVALUATION_PERIOD
start_time = time.time()
transition_times = [start_time + random.randint(0, THRESHOLD_ENGINE_EVALUATION_PERIOD-1)
                    for i in xrange(NUM_METRICS)]
end_times = [None]*NUM_METRICS
values = [0.0]*NUM_METRICS

# Transition metrics based on random transition times
logger.info('randomly transition metrics and wait for all alarms to transition to ALARM')
count = 0
while True:
    current_time = time.time()
    values = map(lambda x: 1.0 if current_time > x else 0.0, transition_times)
    metrics = [create_metric(str(id), values[id]) for id in xrange(NUM_METRICS)]
    send_metrics(monasca_client, metrics)

    alarms = get_alarms(monasca_client)
    for alarm in alarms:
        if alarm['state'] == 'ALARM':
            metrics = alarm['metrics']
            metric = metrics[0]
            dimensions = metric['dimensions']
            id = int(dimensions['id'])
            if end_times[id] == None:
                end_times[id] = time.time()

    transitioned = map(lambda x: x['state'] == 'ALARM', alarms)
    all_transitioned = reduce(lambda x, y: x & y, transitioned)

    if all_transitioned:
        break

    count += 1
    logger.info('check: %d' % count)
    time.sleep(1)
logger.info('done')

elapsed_times = map(lambda x, y: x - y, end_times, transition_times)
mean = np.mean(elapsed_times)
median = np.median(elapsed_times)
std = np.std(elapsed_times)

logger.info('mean: %f' % mean)
logger.info('median: %f' % median)
logger.info('std: %f' % std)