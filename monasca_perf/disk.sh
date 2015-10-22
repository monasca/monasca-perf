#!/bin/bash
# disk.sh
# monitors disk usage
# adapted for HOS environment 

date
df -h | grep /dev/dm-0
du -h /var/kafka | tail -n1
du -h /var/zookeeper | tail -n1
du -h /var/vertica | tail -n1
du -h /var/vertica/data | tail -n1
du -h /var/vertica/catalog | tail -n1
du -h /var/vertica/catalog/mon/v*/vertica.log | tail -n1
du -h /var/vertica/catalog/mon/v*/DataCollector | tail -n1
du -h /var/lib/rabbitmq | tail -n1
du -h /var/log/kafka | tail -n1
du -h /var/log/storm | tail -n1
du -h /var/log/zookeeper | tail -n1
du -h /var/log/ | tail -n1
/opt/vertica/bin/vsql -U dbadmin -w $1 -c "select count(*) from MonMetrics.Measurements"
