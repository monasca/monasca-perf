#!/bin/bash
echo "deleting old log file..."
rm filler_log.txt
declare -r total_num_days=1
for i in $(seq 1 $total_num_days)
do
   echo "Running # $i"
   (pypy ./influxdb_definition_filler.py --day_num $i >> filler_log.txt) &

   if (($i % $total_num_days == 0 ));
      then wait;
   fi
done

