import argparse
import hashlib
import os
import subprocess
import uuid

from collections import defaultdict
from influxdb import InfluxDBClient
from datetime import datetime

NUMBER_OF_MEASUREMENTS = 2000000
NUMBER_PER_BATCH = 5000
NUMBER_OF_HOSTS = 10

TENANT_ID = 'tenant_1'
REGION = 'region_1'


def main(host='localhost', port=8086, client_num=1):
    print("influxDB test start-------------")
    print "host = {}".format(host)
    print "port = {}".format(port)
    print("LINE PROTOCOL")

    hostnames = []
    for i in xrange(0, NUMBER_OF_HOSTS):
        hostnames.append(uuid.uuid4().hex)

    user = 'root'
    password = 'root'
    db_name = 'monasca'
    client = InfluxDBClient(host, port, user, password, db_name)

    running_recording = False
    if db_name not in client.get_list_database():
        print "Create database: ".format(db_name)
        client.create_database(db_name)
        print("Create a retention policy")
        client.create_retention_policy('awesome_policy', '3d', 3, default=True)
        print "Start recording top output"
        running_recording = True
        top_process = subprocess.Popen("exec top -b -d 1 > " + './' + 'system_info', shell=True)

    db_user = 'admin'
    db_user_password = 'my_secret_password'

    print "Switch user: {}".format(db_user)
    client.switch_user(db_user, db_user_password)

    # INSERT
    print "Write points: batch_size = {0}".format(NUMBER_PER_BATCH)
    start_time = datetime.utcnow()
    print "Start time: {0}".format(start_time)

    dimension_keys_values_map = {'service': 'monitoring', 'host': 'localhost',
                                 'cloud': 'cloud_test'}

    print "Inserting {0} measurements".format(NUMBER_OF_MEASUREMENTS)

    metric_count = 0
    metric_name_dict = defaultdict(int)
    for i in xrange(NUMBER_OF_MEASUREMENTS / NUMBER_PER_BATCH):
        batch_set = []
        for j in xrange(NUMBER_PER_BATCH):
            # make sure in each batch, all the measurements have different metric name
            metric_name = 'metric_KS_{0}_{1}'.format(client_num, j)
            metric_name_dict[metric_name] += 1
            value = i * NUMBER_PER_BATCH + j
            dims = dict(dimension_keys_values_map)
            host_name = hostnames[(metric_count / NUMBER_PER_BATCH) % NUMBER_OF_HOSTS]
            dims['host'] = host_name
            dimension_hash_string = ','.join(['%s=%s' % (d, dims[d]) for d in dims])
            new_hash_string = REGION + TENANT_ID + metric_name + dimension_hash_string
            sha1_hash = hashlib.sha1(new_hash_string).hexdigest()
            metric_id = str(sha1_hash)
            line_body = '{0},zone=nova,service=compute,resource_id=34c0ce14-9ce4-4d3d-84a4-172e1ddb26c4,' \
                        'tenant_id=71fea2331bae4d98bb08df071169806d,hostname={1},component=vm,' \
                        'control_plane=ccp,cluster=compute,cloud_name=monasca value={2},' \
                        'metric_id="{3}"'.format(metric_name, host_name, value, str(metric_id))
            batch_set.append(line_body)
            metric_count += 1
        client.write_points(batch_set, batch_size=NUMBER_PER_BATCH,
                            time_precision='ms', protocol='line')
    end_time = datetime.utcnow()
    elapsed = end_time - start_time
    if running_recording:
        os.kill(top_process.pid, 9)
    # Calculate Insert Rate
    print "elapsed time: {0}".format(str(elapsed))
    print "measurements per sec: {0}".format(str(float(NUMBER_OF_MEASUREMENTS) / elapsed.seconds))
    print "metric_name_dict = {}".format(metric_name_dict)


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False, default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    parser.add_argument('--client_num', type=int, required=False, default=1,
                        help='client number')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, client_num=args.client_num)
