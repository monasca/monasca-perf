#!/bin/bash


while :
do
    date >> $1
    sleep 60
    echo "Total Alarm Transitions" >> $1
    /opt/vertica/bin/vsql -U dbadmin -w $2 -c "SELECT count(*) FROM monAlarms.StateHistory WHERE time_stamp < NOW() AND time_stamp >= NOW() - INTERVAL '1 MIN'" >> $1
    echo "Alarm Transitions By ID" >> $1
    /opt/vertica/bin/vsql -U dbadmin -w $2 -c "SELECT alarm_id, count(*) FROM monAlarms.StateHistory WHERE time_stamp < NOW() AND time_stamp >= NOW() - INTERVAL '1 MIN' GROUP BY alarm_id;" >> $1
done
