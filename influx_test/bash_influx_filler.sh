#!/bin/bash
declare -r num_of_runs=1

echo "START"
for i in $(seq 1 $num_of_runs)
do
   echo "Running # $i"
   pypy ./influx_mark2.py
done

echo "DONE"