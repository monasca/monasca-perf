from influxdb import InfluxDBClient
from multiprocessing import Pool

import sys
import time

DATABASE_NAME = 'monasca'
client = InfluxDBClient('localhost', 8086, 'root', 'root', DATABASE_NAME)

print "Creating database: {}...".format(DATABASE_NAME)
client.create_database(DATABASE_NAME)

DB_USER = 'admin'
DB_USER_PASSWORD = 'my_secret_password'

print "Switch user: {}".format(DB_USER)
client.switch_user(DB_USER, DB_USER_PASSWORD)

TOTAL_MEASUREMENT_PROCESSES = 5
MEASUREMENTS_FILENAME = '/tmp/measurements.txt'


def add_measurement_batch(meas_list, filename):
    client1 = InfluxDBClient('localhost', 8086, 'root', 'root', DATABASE_NAME)
    client1.switch_user(DB_USER, DB_USER_PASSWORD)
    client1.write_points(meas_list, batch_size=len(meas_list),
                        time_precision='ms', protocol='line')


def main():
    measurement_process_pool = Pool(TOTAL_MEASUREMENT_PROCESSES)
    measurement_process_id = 0
    for i in xrange(5):
        meas_list = ['cpu.perc,cloud_name=test_cloud1 value=1,id={}'.format(measurement_process_id),
                     'cpu.avg,cloud_name=test_cloud2 value=2,id={}'.format(measurement_process_id),
                     'vswitch.out_errors_sec,tenant_id="f99DbEd937bABDaD46a8",region="Region_1",cloud_name=cloud,cluster=cluster,service=compute,resource_id=4f905288-215c-4d18-9449-3c154b978c2c,zone=nova,component=vm,hostname=test_1,lifespan=10,device=vs3 value=160550 140000000']
        measurement_process_pool.apply_async(add_measurement_batch,
                                             args=(meas_list,
                                                   MEASUREMENTS_FILENAME +
                                                   str(measurement_process_id,)))
        print "measurement_process_id = {}".format(measurement_process_id)
        time.sleep(1)
        measurement_process_id += 1
    print("Waiting for measurement process pool to close")
    measurement_process_pool.close()
    measurement_process_pool.join()

if __name__ == "__main__":
    sys.exit(main())
