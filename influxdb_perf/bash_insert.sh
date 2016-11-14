#!/bin/bash
declare -r num_client=1
# declare -r num_measurements=10000
rm data*
rm system_info

for i in $(seq 1 $num_client)
do
   echo "Running # $i"
   (pypy ./influx_insert_old_line_protocol.py --port 8086 --client_num $i >> data.txt) &

   if (($i % $num_client == 0 ));
      then wait;
   fi
done

sleep 10

echo "Verifying..."
pypy ./influx_verify_data.py --metric_name 'KS' --num_client $num_client

echo "Read system info..."
python ./parser.py

echo "Calculate total insert rate..."
python ./calculate_sum.py
