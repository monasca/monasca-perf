import sys
import time
import threading
import BaseHTTPServer
import json

import kafka
from monascaclient import client
from monascaclient import ksclient

import warnings


# suppress warnings to improve performance
def no_warnings(*args):
    pass
warnings.showwarning = no_warnings

max_wait_time = 20  # seconds

# May result in invalid measurements if disabled when notification engine is
# configured to retry notifications
ack_notifications = True

keystone = {
    'username': 'mini-mon',
    'password': 'password',
    'project': 'test',
    # 'auth_url': 'http://10.22.156.11:35357/v3',
    'auth_url': 'http://192.168.10.5:35357/v3'
}

# monasca api urls
urls = [
    # 'https://mon-ae1test-monasca01.useast.hpcloud.net:8080/v2.0',
    # 'https://mon-ae1test-monasca02.useast.hpcloud.net:8080/v2.0',
    # 'https://mon-ae1test-monasca03.useast.hpcloud.net:8080/v2.0',
    'http://192.168.10.4:8080/v2.0',
]

# requires ip and port (default port is 9092)
kafka_host = '192.168.10.4:9092'
# kafka_hosts = ['10.22.156.11:9092','10.22.156.12:9092','10.22.156.13:9092']
kafka_topic = "alarm-state-transitions"

# server to catch the webhooks
webhook_server_config = ('192.168.10.4', 8001)
# webhook_server_config = ('10.22.156.11', 8001)

notification_method = {
    'name': 'Test',
    'type': 'WEBHOOK',
    'address': 'http://{}:{}'.format(webhook_server_config[0],
                                     webhook_server_config[1])
}

alarm_definition = {
    'name': 'Test223',
    'expression': 'alarm_perf < 10',
    'alarm_actions': [],
    'ok_actions': [],
    'undetermined_actions': []
}

base_message = {
    "alarm-transitioned":
        {"tenantId": "nothing",
         "alarmId": "noID",
         "alarmDefinitionId": "notAnID",
         "metrics":
             [{"id": "null",
               "name": "this is a test",
               "dimensions":
                   {"service": "monitoring",
                    "hostname": "mini-mon"}}],
         "alarmName": "TestingTesting",
         "alarmDescription": "This is a test of the notification engine",
         "oldState": "UNDETERMINED",
         "newState": "OK",
         "actionsEnabled": "true",
         "stateChangeReason": "Because I made it so",
         "severity": "LOW",
         "timestamp": 1422918282}}

response_count = 0
last_response = 0
stop_server = False


class TestHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        global response_count
        global last_response
        response_count += 1
        last_response = time.time()
        if ack_notifications:
            self.send_response(200, 'OK')

    def do_nothing(self, *args):
        pass

    log_message = do_nothing


def check_notifications(server_class=BaseHTTPServer.HTTPServer,
                        handler_class=TestHTTPRequestHandler):
    httpd = server_class(webhook_server_config, handler_class)
    httpd.timeout = 1
    while response_count < 1 and not stop_server:
        httpd.handle_request()


def create_notification_method(mon_client):
    for notification in mon_client.notifications.list():
        if (notification['type'] == notification_method['type'] and
                notification['address'] == notification_method['address']):
            print("Already exists, ID: {}".format(notification['id']))
            return notification['id']
    try:
        resp = mon_client.notifications.create(**notification_method)
        print('Notification Method ID: {}'.format(resp['id']))
        return resp['id']
    except Exception as ex:
        print('Could not create notification method: {}'.format(ex))
        return None


def create_alarm_definition(mon_client):
    for definition in mon_client.alarm_definitions.list():
        if definition['name'] == alarm_definition['name']:
            mon_client.alarm_definitions.delete(alarm_id=definition['id'])
    try:
        resp = mon_client.alarm_definitions.create(**alarm_definition)
        print('Alarm Definition ID: {}'.format(resp['id']))
        return resp['id']
    except Exception as ex:
        print('Could not create alarm definition: {}'.format(ex))
        return None


def produce_alarm_state_transition():
    kafka_client = kafka.client.KafkaClient(kafka_host)
    kafka_producer = kafka.producer.SimpleProducer(kafka_client, async=False)

    base_message["alarm-transitioned"]["timestamp"] = int(time.time())
    kafka_producer.send_messages(kafka_topic,
                                 json.dumps(base_message))


def notification_function_test():
    global last_response
    global response_count
    global stop_server

    try:
        print('Authenticating with keystone on {}'.
              format(keystone['auth_url']))
        ks_client = ksclient.KSClient(**keystone)
    except Exception as ex:
        print('Failed to authenticate: {}'.format(ex))
        return False

    token = ks_client.token

    mon_client = client.Client('2_0', urls[0], token=token)

    print("Creating notification method")
    notification_id = create_notification_method(mon_client)
    if not notification_id:
        return False

    alarm_definition['ok_actions'].append(notification_id)
    alarm_definition['alarm_actions'].append(notification_id)
    alarm_definition['undetermined_actions'].append(notification_id)

    print("Creating alarm definition")
    alarm_def_id = create_alarm_definition(mon_client)
    if not alarm_def_id:
        return False

    base_message['alarm-transitioned']['alarmDefinitionId'] = alarm_def_id

    server = threading.Thread(target=check_notifications,
                              args=(BaseHTTPServer.HTTPServer,
                                    TestHTTPRequestHandler))
    server.start()

    time.sleep(1)

    start_time = time.time()

    produce_alarm_state_transition()

    last_response = time.time()

    print("Waiting for notifications")
    while server.isAlive():
        if(last_response + max_wait_time) < time.time():
            stop_server = True
            print("Max wait time exceeded after {} responses".
                  format(response_count))
            return False
        server.join((last_response+max_wait_time)-time.time())

    final_time = time.time()

    print("-----Test Results-----")
    print("{} notifications arrived in {} seconds".
          format(response_count, final_time-start_time))
    return True


def main():
    if not notification_function_test():
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
