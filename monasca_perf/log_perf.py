# (C) Copyright 2016 HP Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import time

import multiprocessing
import requests
import urlparse
import ujson as json

SIZE_LOG_MSG = 1024

# Uncomment to simulate 4 log messages per request
# NUM_PROCESSES = 100
# NUM_REQUESTS_PER_PROCESS = 250
# NUM_LOG_MESSAGES_PER_REQUEST = 4

# Uncomment to simulate 100 log messages per request
NUM_PROCESSES = 100
NUM_REQUESTS_PER_PROCESS = 10
NUM_LOG_MESSAGES_PER_REQUEST = 100

TOTAL_MESSAGES = NUM_PROCESSES * NUM_REQUESTS_PER_PROCESS * \
                 NUM_LOG_MESSAGES_PER_REQUEST

MONASCA_LOG_API_URL = 'http://127.0.0.1:8082/v3.0/logs'

# Uncomment for monasca-vagrant environment.
# KEYSTONE_API_URL = 'http://192.168.10.5:35357/v3/auth/tokens'
# USERNAME = 'mini-mon'
# DOMAIN_NAME = 'default'
# PASSWORD = 'password'
# PROJECT_NAME = 'mini-mon'

# Uncomment for DevStack environment.
KEYSTONE_API_URL = 'http://192.168.10.6:35357/v3/auth/tokens'
USERNAME = 'admin'
DOMAIN_NAME = 'default'
PASSWORD = 'secretadmin'
PROJECT_NAME = 'admin'


def get_keystone_token():
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    request_body = {"auth": {
        "identity": {
            "methods": ["password"],
            "password": {
                "user": {
                    "name": USERNAME,
                    "domain": {"id": DOMAIN_NAME},
                    "password": PASSWORD
                }
            }
        },
        "scope": {
            "project": {
                "name": PROJECT_NAME,
                "domain": {"id": DOMAIN_NAME}
            }
        }
    }
    }
    request_body = json.dumps(request_body)
    response = requests.post(KEYSTONE_API_URL, data=request_body,
                             headers=headers)
    return response


def send_requests_per_process(token):
    headers = {"Content-type": "application/json",
               "X-Auth-Token": token}
    for request_id in xrange(NUM_REQUESTS_PER_PROCESS):
        session = requests.Session()
        response = send_logs_per_request(session, request_id, headers)
        handle_response(response)
    return


def send_logs_per_request(session, request_id, headers):
    logs = [{"msg": "X" * SIZE_LOG_MSG} for i in
            xrange(NUM_LOG_MESSAGES_PER_REQUEST)]
    dimensions = {"application": "application" + str(request_id),
                  "hostname": "hostname" + str(request_id) + ".domain.com"}
    request_body = {
        "dimensions": dimensions,
        "logs": logs
    }
    request_body = json.dumps(request_body)

    try:
        response = session.post(MONASCA_LOG_API_URL, data=request_body,
                                headers=headers, timeout=None)
        if response.status_code != 204:
            raise Exception(response.status_code)
        return response
    except Exception as ex:
        print ex
        raise


def handle_response(res):
    pass


token = get_keystone_token().headers['x-subject-token']

start_time = time.time()

process_list = [multiprocessing.Process(target=send_requests_per_process,
                                        args=[token])
                for i in xrange(NUM_PROCESSES)]
map(lambda x: x.start(), process_list)
map(lambda x: x.join(), process_list)

end_time = time.time()
elapsed_time = end_time - start_time
num_messages_per_sec = TOTAL_MESSAGES / elapsed_time

# Output details
print "num_processes: %d" % NUM_PROCESSES
print "num_requests_per_process: %d" % NUM_REQUESTS_PER_PROCESS
print "num_logs_per_request: %d" % NUM_LOG_MESSAGES_PER_REQUEST
print "total_messages: %s" % TOTAL_MESSAGES
print "elapsed_time: %f" % elapsed_time
print "num_messages_per_sec: %f" % num_messages_per_sec
