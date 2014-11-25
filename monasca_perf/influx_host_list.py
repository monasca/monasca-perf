import urllib, sys
import re
import base64
from influxdb import client as influxdb

username = sys.argv[1]
password = sys.argv[2]
url = sys.argv[3]

db = influxdb.InfluxDBClient(url, 8086, username, password, 'mon')
series_list = db.query('list series;')

hosts = set()
bad_series = 0
total_series = 0
hosts_amplified = set()
for series in series_list[0]["points"]:
    total_series += 1
    series_name = series[1]
    series_items = {}
    series_split = re.split(r'&',series_name)
    for series_name_item in series_split:
        series_name_item_split = re.split(r'=',series_name_item)
        try:
            series_items[series_name_item_split[0]] = series_name_item_split[1]
        except IndexError:
            pass
    hostname = ""
    try:
        hostname = series_items['hostname']
    except KeyError:
        try:
            hostname = series_items['instance_id']
        except KeyError:
            print series_name
            bad_series += 1
            continue
    if len(hostname) == 0:
        print series_name
        continue
    amplifier = ""
    try:
        amplifier = series_items['amplifier']
    except KeyError:
        pass
    hostname_amplified = hostname
    if len(amplifier) > 0:
        hostname_amplified += ':amplifier:'
        hostname_amplified += amplifier
    hosts.add(hostname)
    hosts_amplified.add(hostname)
    hosts_amplified.add(hostname_amplified)

print
print "Num no host series: ", bad_series
for item in sorted(list(hosts)):
    print item
print "Num hosts: ", len(hosts)
print
for item in sorted(list(hosts_amplified)):
    print item
print "Num hosts + amplified: ", len(hosts_amplified)
print
print "Num total time series: ", total_series
