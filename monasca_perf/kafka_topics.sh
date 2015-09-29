#!/bin/bash

while :
do
	date >> kafka_info
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect localhost:2181 --topic alarm-state-transitions --group 1_alarm-state-transitions >> kafka_info
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect localhost:2181 --topic metrics --group thresh-metric >> kafka_info
	/opt/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zkconnect localhost:2181 --topic metrics --group 1_metrics >> kafka_info
	sleep 30
done

