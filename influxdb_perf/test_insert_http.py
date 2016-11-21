import os
import subprocess
import sys

DATABASE_NAME = 'monasca'
DB_USER = 'admin'
DB_USER_PASSWORD = 'my_secret_password'

check_db_cmd = 'curl -i -XPOST http://localhost:8086/query --data-urlencode "q=SHOW DATABASES"'
db_result = subprocess.Popen(check_db_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
out, err = db_result.communicate()
print "out = {}".format(out[0])


cmd = 'curl -i -XPOST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE monasca"'
subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
TOTAL_MEASUREMENT_PROCESSES = 5
MEASUREMENTS_FILENAME = '/tmp/measurements.txt'


def main():
    line_protocol = 'cpu_load_short,host=server02 value=0.67 1434055562000000000' + '\n' + 'cpu_load_short,host=server02,region=us-west value=0.55 1434055562000000001' + '\n' + 'cpu_load_short,direction=in,host=server01,region=us-west value=2.0'
    write_cmd2 = 'curl -i -XPOST "http://localhost:8086/write?db=monasca" --data-binary "{}"'.format(line_protocol)
    subprocess.Popen(write_cmd2, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

if __name__ == "__main__":
    sys.exit(main())
