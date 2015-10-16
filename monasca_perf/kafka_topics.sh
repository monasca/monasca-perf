#!/bin/bash

while :
do
	date >> $1
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect localhost:2181 --topic alarm-state-transitions --group 1_alarm-state-transitions >> $1
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect localhost:2181 --topic metrics --group thresh-metric >> $1
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect localhost:2181 --topic metrics --group 1_metrics >> $1
	sleep 30
done

