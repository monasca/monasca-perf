import json
import requests
import time

resource_id = '0a988066-810e-4b23-a159-c733938f8614'
timestamp = '1476546163030'
limit = 10000


def main():
    print "influxDB query test start-------------"
    url = 'http://localhost:8086/query'
    param = 'q=CREATE DATABASE monasca'
    requests.get(url=url, params=param)

    # Query Metrics
    print("No filters query")
    query = 'q=SHOW SERIES limit {}'.format(limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Name only ")
    query = 'q=SHOW SERIES FROM "io.write_bytes_total_sec" limit {}'.format(limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Name and start time")
    query = 'q=SELECT * FROM "io.write_bytes_total_sec" where time >= {} limit {}'.format(
        timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Name and resource_id (single result)")
    query = 'q=SHOW SERIES FROM "io.write_bytes_total_sec" WHERE ' \
            'resource_id=\'{}\' limit {}'.format(resource_id, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Name and max dimensions (max results)")
    query = 'q=SHOW SERIES FROM "io.write_bytes_total_sec" WHERE cloud_name=\'test_cloud\' and ' \
            'cluster=\'test_cluster\' and component=\'vm\' and hostname=\'test_1200\' and ' \
            'lifespan=\'1200\' and service=\'compute\' and zone=\'nova\' limit {}'.format(limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Dimensions only, resource_id (single vm results)")
    query = 'q=SHOW SERIES WHERE resource_id=\'{}\' limit {}'.format(resource_id, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Dimensions only, resource_id and device (single device result)")
    query = 'q=SHOW SERIES WHERE resource_id=\'{}\' and ' \
            'device=\'vs1\' limit {}'.format(resource_id, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Dimensions only, device (one device over multiple vms result)")
    query = 'q=SHOW SERIES WHERE device=\'vs1\' limit {}'.format(limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    # Query Measurements
    print("\nMeasurement-list | Name only merged (non-vm metric)")
    query = 'q=SELECT * FROM "cpu.time_ns" where time >= {} limit {}'.format(timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMeasurement-list | Name only grouped (non-vm metric)")
    query = 'q=SELECT * FROM "cpu.time_ns" where time >= {} group by * limit {}'.format(
        timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMeasurement-list | Name only merged (vm metric)")
    query = 'q=SELECT * FROM "vm.mem.used_mb" where time >= {} limit {}'.format(timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMeasurement-list | Name only grouped (vm metric)")
    query = 'q=SELECT * FROM "vm.mem.used_mb" where time >= {} group by * limit {}'.format(
        timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMeasurement-list | Name and resource_id (single result)")
    query = 'q=SELECT * FROM "vm.mem.used_mb" where time >= {0} and ' \
            'resource_id=\'{1}\' limit {2}'.format(timestamp, resource_id, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMeasurement-list | Name and max dimensions query (max results)")
    query = 'q=SELECT * FROM "vm.mem.used_mb" where time >= {} and ' \
            'cloud_name=\'test_cloud\' and cluster=\'test_cluster\' and component=\'vm\' and ' \
            'hostname=\'test_1200\' and lifespan=\'1200\' and service=\'compute\' and ' \
            'zone=\'nova\' limit {}'.format(timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    # Query Statistics
    print("\nMetric-statistics | name only merged (non-vm metric)")
    query = 'q=SELECT max(value) from "cpu.time_ns" where time >= {} limit {}'.format(
        timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-statistics | name only merged (vm metric)")
    query = 'q=SELECT max(value) from "vm.mem.free_perc" where time >= {} limit {}'.format(
        timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-statistics | name and resource_id (single result)")
    query = 'q=SELECT max(value) from "vm.mem.free_perc" where time >= {} and ' \
            'resource_id=\'{}\' limit {}'.format(timestamp, resource_id, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-statistics | name and max dimensions query (max results)")
    query = 'q=SELECT max(value) FROM "vm.mem.used_mb" where time >= {} and ' \
            'cloud_name=\'test_cloud\' and cluster=\'test_cluster\' and component=\'vm\' and ' \
            'hostname=\'test_1200\' and lifespan=\'1200\' and service=\'compute\' and ' \
            'zone=\'nova\' limit {}'.format(timestamp, limit)
    r, delta_time = run_query(query)
    status_output(r, delta_time)


def run_query(query_param):
    query_url = 'http://localhost:8086/query?db=monasca'
    start_time = time.time()
    r = requests.get(url=query_url, params=query_param)
    delta_time = time.time() - start_time
    return r, delta_time


def status_output(req, delta_time):
    req_text_dict = json.loads(req.text)
    if len(req_text_dict['results']) >= 1:
        result = req_text_dict['results'][0]
        if result.has_key('error'):
            print "ERROR: {}".format(req.text.encode())
        else:
            print "query_time_sec = {}\n".format(delta_time)

if __name__ == '__main__':
    main()
