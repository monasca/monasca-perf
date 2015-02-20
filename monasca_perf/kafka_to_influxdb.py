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

influx_url = "http://192.168.10.4:8086/db/mon/series?u=root&p=root"

kafka_hosts = ['192.168.10.4:9092']
#kafka_hosts = ['10.22.156.11:9092','10.22.156.12:9092','10.22.156.13:9092']
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
        return 0

    final_time = time.time()
    print("Sent {} in {} seconds".format(num_processes*num_messages, final_time-start_time))

    series_name = build_series_name(series['token'],
                                    series['region'],
                                    series['metric_name'], series['metric_dimensions'])
    influx_query = "select count(value) from {} where time>{}".format(series_name, int(start_time*1000))

    # query influxdb until metrics arrive
    #url = urlparse.urlparse(influx_url)
    #conn = httplib.HTTPConnection(url.netloc)
    #count = 0
    #query_url = influx_url + "&q=" + influx_query
    #print(query_url)
    #while count < num_processes*num_messages:
    #    conn.request("GET", query_url)
    #    res = conn.getresponse()
    #    if res.status != 204:
    #        raise Exception(res.status)
    #    result = res.read()
    #    result = json.loads(result)
    #    print(result)
    #    time.sleep(1)

    client = influxdb.InfluxDBClient("192.168.10.4", 8086, "root", "root", "mon")
    resp = client.query(influx_query)
    count = resp[0]['points'][0][1]

    final_time = time.time()
    print("{} metrics from influx in {} seconds".format(count, final_time-start_time))

kafka_to_influxdb_test()