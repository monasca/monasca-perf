import argparse
import hashlib
import uuid

from influxdb import InfluxDBClient
from datetime import datetime

NUMBER_OF_MEASUREMENTS_TO_INSERT = 2000000
NUMBER_OF_UNIQUE_METRICS = 1000
NUMBER_PER_BATCH = 5000
NUMBER_OF_HOSTS = 10

TENANT_ID = 'tenant_1'
REGION = 'region_1'


def main(host='localhost', port=8086):
    print("influxDB test start-------------")
    print("JSON PROTOCOL")
    hostnames = []
    for i in xrange(0, NUMBER_OF_HOSTS):
        hostnames.append(uuid.uuid4().hex)

    user = 'root'
    password = 'root'
    db_name = 'monasca'
    client = InfluxDBClient(host, port, user, password, db_name)

    # print("Create database: " + db_name)
    client.create_database(db_name)

    db_user = 'test_user'
    db_user_password = 'my_secret_password'

    print("Switch user: " + db_user)
    client.switch_user(db_user, db_user_password)

    # INSERT
    print "Write points: batch_size = {0}".format(NUMBER_PER_BATCH)
    start_time = datetime.utcnow()
    print "Start time: {0}".format(start_time)

    dimension_keys_values_map = {'service': 'monitoring', 'host': 'localhost',
                                 'cloud': 'cloud_test'}

    print "Inserting {0} measurements".format(NUMBER_OF_MEASUREMENTS_TO_INSERT)

    metric_count = 0
    for i in xrange(NUMBER_OF_MEASUREMENTS_TO_INSERT / NUMBER_PER_BATCH):
        batch_set = []
        for j in xrange(NUMBER_PER_BATCH):
            metric_suffix = metric_count % NUMBER_OF_UNIQUE_METRICS
            metric_name = 'metric_KS_' + str(metric_suffix)
            value = i * NUMBER_PER_BATCH + j
            dims = dict(dimension_keys_values_map)
            dims['host'] = hostnames[(metric_count / NUMBER_OF_UNIQUE_METRICS) % NUMBER_OF_HOSTS]
            dimension_hash_string = ','.join(['%s=%s' % (d, dims[d]) for d in dims])
            new_hash_string = REGION + TENANT_ID + metric_name + dimension_hash_string
            sha1_hash = hashlib.sha1(new_hash_string).hexdigest()
            metric_id = str(sha1_hash)
            json_body = {
                   "measurement": metric_name,
                   "tags": dims,
                   "time": datetime.utcnow(),
                   "fields": {
                       "value": value,
                       "metric_id": metric_id
                   }
               }
            batch_set.append(json_body)
            metric_count += 1
        client.write_points(batch_set, batch_size=NUMBER_PER_BATCH,
                            time_precision='ms')

    end_time = datetime.utcnow()
    elapsed = end_time - start_time

    # Calculate Insert Rate
    print "elapsed time: {0}".format(str(elapsed))
    print "measurements per sec: {0}".format(str(float(
        NUMBER_OF_MEASUREMENTS_TO_INSERT) / elapsed.seconds))


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False, default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port)

