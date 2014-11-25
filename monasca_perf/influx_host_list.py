#!/usr/bin/env python
from collections import defaultdict
import sys
from influxdb import client as influxdb

username = sys.argv[1]
password = sys.argv[2]
hostname = sys.argv[3]

db = influxdb.InfluxDBClient(hostname, 8086, username, password, 'mon')

hosts = set()
hosts_amplified = defaultdict(int)
total_metrics = 0
metrics_missing_hostname = 0
for series in db.query('list series;')[0]["points"]:
    total_metrics += 1
    entries = [entry for entry in series[1].split('&')]
    series_items = {entry.split('=')[0]: entry.split('=')[1] for entry in entries if entry.find('=') != -1}

    if series_items.has_key('hostname'):
        hosts.add(series_items['hostname'])
    else:
        metrics_missing_hostname += 1

    if series_items.has_key('amplifier'):
        hosts_amplified[series_items['hostname']] += 1

print('Found %d hosts' % len(hosts))
print('Found %d amplified hosts' % len(hosts_amplified))
print('Found %d total metrics with %d metrics missing hostnames. Making an average %f metrics per host' %
      (total_metrics, metrics_missing_hostname, total_metrics/len(hosts)))

print("\nAmplified host list:")
for host in hosts_amplified.iterkeys():
    print(host)

print("\nHosts in aw1 but not in the amplified list.")
unamplified_count = 0
for host in hosts:
    if not hosts_amplified.has_key(host) and host.find('aw1') != -1:
        print(host)
        unamplified_count += 1
print('Total unamplified %d' % unamplified_count)