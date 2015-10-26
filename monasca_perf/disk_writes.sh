#!/bin/bash

while :
do
	date >> $1
	iostat >> $1
	sleep 15
done
