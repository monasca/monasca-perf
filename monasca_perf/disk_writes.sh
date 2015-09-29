#!/bin/bash

while :
do
	date >> iostat_metrics
	iostat >> iostat_metrics
	sleep 15
done
