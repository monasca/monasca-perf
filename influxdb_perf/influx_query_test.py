import json
import requests
import time


def main():
    print "influxDB query test start-------------"
    url = 'http://localhost:8086/query'
    param = 'q=CREATE DATABASE monasca'
    requests.get(url=url, params=param)

    print("No filters query")
    query = 'q=SHOW SERIES'
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Name only ")
    query = 'q=SHOW SERIES FROM "io.write_bytes_total_sec"'
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    # print("\nMetric-list | Name and start time")
    # query = 'q=SHOW SERIES FROM "io.write_bytes_total_sec" where time >= 1476085793030'
    # r, delta_time = run_query(query)
    # status_output(r, delta_time)

    print("\nMetric-list | Name and max dimensions (single result)")
    query = 'q=SHOW SERIES FROM "io.write_bytes_total_sec" WHERE resource_id=\'a17d0d28-0c3e-48d5-9554-9ae86a788229\''
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Name and max dimensions (max results)")
    query = 'q=SHOW SERIES FROM "io.write_bytes_total_sec" WHERE cloud_name=\'test_cloud\' and ' \
            'cluster=\'test_cluster\' and component=\'vm\' and hostname=\'test_1000\' and ' \
            'lifespan=\'1200\' and service=\'compute\' and zone=\'nova\''
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Dimensions only, resource_id (single vm results)")
    query = 'q=SHOW SERIES WHERE resource_id=\'a17d0d28-0c3e-48d5-9554-9ae86a788229\''
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Dimensions only, resource_id and device (single device result)")
    query = 'q=SHOW SERIES WHERE resource_id=\'a17d0d28-0c3e-48d5-9554-9ae86a788229\' and ' \
            'device=\'vs1\''
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMetric-list | Dimensions only, device (one device over multiple vms result)")
    query = 'q=SHOW SERIES WHERE device=\'vs1\''
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    # print("\nMeasurement-list | Name only merged (non-vm metric)")
    # query = 'q=SELECT * FROM "cpu.idle_perc" where time >= 1476085793030 merge resource_id'
    # r, delta_time = run_query(query)
    # status_output(r, delta_time)

    print("\nMeasurement-list | Name only grouped (non-vm metric)")
    query = 'q=SELECT * FROM "cpu.idle_perc" where time >= 1476085793030 group by *'
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    # print("\nMeasurement-list | Name only merged (vm metric)")
    print("\nMeasurement-list | Name only grouped (vm metric)")
    query = 'q=SELECT * FROM "vm.mem.used_mb" where time >= 1476085793030 group by *'
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMeasurement-list | Name and resource_id (single result)")
    query = 'q=SELECT * FROM "vm.mem.used_mb" where time >= 1476085793030 and ' \
            'resource_id=\'1b87f5c7-c541-4d2b-932f-117e03c57514\''
    r, delta_time = run_query(query)
    status_output(r, delta_time)

    print("\nMeasurement-list | Name and max dimensions query (max results)")
    query = 'q=SELECT * FROM "vm.mem.used_mb" where time >= 1476085793030 and ' \
            'cloud_name=\'test_cloud\' and cluster=\'test_cluster\' and component=\'vm\' and ' \
            'hostname=\'test_1000\' and lifespan=\'1200\' and service=\'compute\' and zone=\'nova\''
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
