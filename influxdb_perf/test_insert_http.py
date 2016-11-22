import requests
import sys

DATABASE_NAME = 'monasca'
DB_USER = 'admin'
DB_USER_PASSWORD = 'my_secret_password'

url = 'http://localhost:8086/query'
param = 'q=CREATE DATABASE monasca'
r = requests.get(url=url, params=param)
print "r.url = {}".format(r.url)


def main():
    url1 = 'http://localhost:8086/write?db=monasca'
    r1 = requests.post(url1, data='cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000' + '\n' + 'cpu_load_short,host=server02,region=us-west value=0.55 1434055562000000001')
    print "r1.url = {}".format(r1.url)
    print r1.status_code
    print r1.text


if __name__ == "__main__":
    sys.exit(main())
