import argparse

from influxdb import InfluxDBClient

NUMBER_OF_UNIQUE_METRICS = 5000


def main(host='localhost', port=8086, metric_name='KS', num_clients=4):
    print "influxDB verification start-------------"
    print "LINE PROTOCOL"
    user = 'root'
    password = 'root'
    db_name = 'monasca'
    client = InfluxDBClient(host, port, user, password, db_name)

    db_user = 'admin'
    db_user_password = 'my_secret_password'

    print "Switch user: {}".format(db_user)
    client.switch_user(db_user, db_user_password)

    total_measurements = 0
    for i in xrange(1, num_clients + 1):
        measurements_per_client = 0
        for j in xrange(NUMBER_OF_UNIQUE_METRICS):
            query = 'SELECT count(value) FROM /metric_{0}_{1}_{2}/'.format(metric_name, i, j)
            result = client.query(query)
            metric_point = list(result.get_points(measurement='metric_{0}_{1}_{2}'.format(
                metric_name, i, j)))
            count_result = metric_point[0]['count']
            measurements_per_client += count_result
        print "{0} measurements per client # {1} = {2}".format(
            metric_name, i, measurements_per_client)
        total_measurements += measurements_per_client
    print "total {0} measurements = {1}".format(metric_name, total_measurements)


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False, default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    parser.add_argument('--metric_name', type=str, required=False, default='KS',
                        help='Partial of metric name')
    parser.add_argument('--num_clients', type=int, required=False, default=1,
                        help='num_clients')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, metric_name=args.metric_name, num_clients=args.num_clients)
