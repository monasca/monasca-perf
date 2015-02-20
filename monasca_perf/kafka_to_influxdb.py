__author__ = 'ryan'

import httplib
import json
import multiprocessing
import time
import urlparse
import urllib

import influxdb
import kafka



num_processes = 1
num_messages = 1

max_wait_time = 20  # Seconds

base_message = {
    "metric": {
        "name":"monasca.emit_time_sec",
        "dimensions":{
            "component":"monasca-agent",
            "hostname":"devstack"},
        "timestamp":1424375781,
        "value":0.007759809494018555},
    "meta":{
        "tenantId":"762d8d35734d425d8a19e1f3e4fbd5d7",
        "region":"useast"},
    "creation_time":1424375787}

series = {
    'token': '762d8d35734d425d8a19e1f3e4fbd5d7',
    'region': 'useast',
    'metric_name': 'monasca.emit_time_sec',
    'metric_dimensions': {
            "component":"monasca-agent",
            "hostname":"devstack"}
}

influx_config = {
    "ip": "192.168.10.4",
    "port": 8086,
    "username": "root",
    "password": "root",
    "database": "mon"
}

kafka_hosts = ['192.168.10.4:9092']
kafka_topic = "metrics"


def produce_messages(pid):
    host = pid % len(kafka_hosts)
    kafka_client = kafka.client.KafkaClient(kafka_hosts[host])
    kafka_producer = kafka.producer.KeyedProducer(kafka_client, async=False)

    for i in range(0,num_messages):
        try:
            kafka_producer.send_messages(kafka_topic, pid % 4, json.dumps(base_message))

        except Exception:
            pass


def build_series_name(token, region, metric_name, dimensions):
    result = "\"{0}?{1}&{2}".format(token, region, metric_name)
    sorted_dimensions = []
    for key in dimensions:
        sorted_dimensions.append((key,dimensions[key]))
    sorted_dimensions.sort()
    result = result + "&" + urllib.urlencode(sorted_dimensions) + "\""
    return result


def aggregate_sent_count(sent_q):
    total_sent = 0
    while not sent_q.empty():
        item = sent_q.get()
        if isinstance(item,int):
            total_sent += item
        else:
            print(item)
    return total_sent


def kafka_to_influxdb_test():
    process_list = []

    start_time = time.time()
    print("Sending {} messages".format(num_processes*num_messages))
    for i in xrange(num_processes):
        p = multiprocessing.Process(target=produce_messages, args=(i,))
        process_list.append(p)
        p.start()

    try:
        for p in process_list:
            try:
                p.join()
            except Exception:
                pass

    except KeyboardInterrupt:
        return False

    final_time = time.time()
    print("Sent {} in {} seconds".format(num_processes*num_messages, final_time-start_time))

    series_name = build_series_name(series['token'],
                                    series['region'],
                                    series['metric_name'], series['metric_dimensions'])
    influx_query = "select count(value) from {} where time>{}".format(series_name, int(start_time*1000))

    count = 0
    last_change = time.time()
    while count < num_processes*num_messages:
        client = influxdb.InfluxDBClient(influx_config['ip'],
                                         influx_config['port'],
                                         influx_config['username'],
                                         influx_config['password'],
                                         influx_config['database'])
        resp = client.query(influx_query)
        temp = count
        count = resp[0]['points'][0][1]
        if(temp != count):
            last_change = time.time()
        if (last_change + max_wait_time) <= time.time():
            return False
        time.sleep(1)

    final_time = time.time()
    print("{} metrics from influx in {} seconds".format(count, final_time-start_time))
    return True

def main():
    if not kafka_to_influxdb_test():
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())