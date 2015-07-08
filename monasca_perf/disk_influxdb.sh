#!/bin/bash
while :
do
	date
	systemctl status influxdb
        df -h | grep /dev/mapper/vg00-root
        du -h /var/opt/influxdb | tail -n1
        du -h /var/kafka | tail -n1
        du -h /var/log/kafka | tail -n1
        du -h /var/log/storm | tail -n1
        du -h /var/log/zookeeper | tail -n1
        du -h /var/log/ | tail -n1
#        python influxdb_size.py | grep measure
	sleep 60
done
