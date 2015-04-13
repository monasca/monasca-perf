#!/bin/sh
#This is for updating to the latest 0.9.0 RC on each node. Takes 1 argument, the RC number.
sudo service influxdb stop
sleep 15
wget "http://s3.amazonaws.com/influxdb/influxdb_0.9.0-rc$1_amd64.deb"
sudo service influxdb stop
sudo dpkg -r influxdb
sudo rm -rf /var/opt/influxdb
sudo rm -rf /opt/influxdb/
sudo dpkg -i "influxdb_0.9.0-rc$1_amd64.deb"
