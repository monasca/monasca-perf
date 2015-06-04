#!/bin/bash
while :
do
	date
        df -h | grep /dev/vda1
        du -h /var/kafka | tail -n1
        du -h /var/vertica | tail -n1
        du -h /var/log/kafka | tail -n1
        du -h /var/log/storm | tail -n1
        du -h /var/log/zookeeper | tail -n1
        du -h /var/log/ | tail -n1
        /opt/vertica/bin/vsql -U dbadmin -w  password  -c "select count(*) from MonMetrics.Measurements"
	sleep 60
done
