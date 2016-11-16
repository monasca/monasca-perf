import argparse
import os

from influxdb import InfluxDBClient


def main(db='monasca'):
    print("Dropping Database: {}".format(db))
    port = '8086'
    host = 'localhost'
    user = 'root'
    password = 'root'
    db_name = db
    client = InfluxDBClient(host, port, user, password, db_name)
    client.drop_database(db)


def parse_args():
    parser = argparse.ArgumentParser(
        description='drop an influxdb, used for perf testing')
    parser.add_argument('--db', type=str, required=False, default='monasca',
                        help='Database Name')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    main(db=args.db)
