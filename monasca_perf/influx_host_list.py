#!/usr/bin/env python
import sys
from influxdb import client as influxdb

username = sys.argv[1]
password = sys.argv[2]
hostname = sys.argv[3]

db = influxdb.InfluxDBClient(hostname, 8086, username, password, 'mon')

hosts = set()
hosts_amplified = {}
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

    if series_items.has_key('amplifier'):  # I assume if it has a amplifier it has a hostname also
        hostname = series_items['hostname']
        if not hosts_amplified.has_key(hostname) or hosts_amplified[hostname] < int(series_items['amplifier']):
            hosts_amplified[hostname] = int(series_items['amplifier'])

print('Found %d hosts' % len(hosts))
print('Found %d amplified hosts' % len(hosts_amplified))
amplification = hosts_amplified[hosts_amplified.keys()[0]]
virtual_hosts = len(hosts) + (amplification * len(hosts_amplified))
print('Total + amplified hosts = %d virtual hosts - caculated with amplification %d' % (virtual_hosts, amplification))
print('Found %d total metrics with %d metrics missing hostnames. Making an average %f metrics per host' %
      (total_metrics, metrics_missing_hostname, float(total_metrics)/virtual_hosts))

print("\nHosts in aw1 but not in the amplified list.")
unamplified_count = 0
not_fully_amplified_count = 0
for host in hosts:
    if not hosts_amplified.has_key(host) and host.find('aw1') != -1:
        print(host)
        unamplified_count += 1
    if hosts_amplified.has_key(host) and hosts_amplified[host] != amplification:
        not_fully_amplified_count += 1
print('Total unamplified %d' % unamplified_count)
print('Total amplified but not at reported amplification (%d) = %d' % (amplification, not_fully_amplified_count))
