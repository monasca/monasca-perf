import json
from monascaclient import client
from monascaclient import ksclient

# list of metrics to collect (full name, identifying dimensions minus hostnames)
# metrics = [{"name":"metrics.published", "dimensions":{"service":"monitoring","component":"api"}},
#           {"name":"metrics-added-to-batch-counter[0]", "dimensions":{"service":"monitoring","component":"persister"}},
#           {"name":"metrics-added-to-batch-counter[1]", "dimensions":{"service":"monitoring","component":"persister"}},
#           {"name":"mysql.innodb.mutex_spin_rounds", "dimensions":{}},
#           {"name":"mysql.net.connections", "dimensions":{}},
#           {"name":"mysql.performance.com_update_multi", "dimensions":{}},
#           {"name":"mysql.performance.questions", "dimensions":{}},
#           {"name":"mysql.performance.created_tmp_files", "dimensions":{}},
#           {"name":"mysql.performance.user_time", "dimensions":{}},
#           {"name":"mysql.performance.kernel_time", "dimensions":{}},
#           {"name":"mysql.innodb.buffer_pool_free", "dimensions":{}},
#           {"name":"mysql.innodb.data_writes", "dimensions":{}},
#           {"name":"mysql.performance.com_delete", "dimensions":{}},
#           {"name":"mysql.innodb.mutex_os_waits", "dimensions":{}},
#           {"name":"mysql.performance.queries", "dimensions":{}},
#           {"name":"mysql.innodb.buffer_pool_total", "dimensions":{}},
#           {"name":"mysql.innodb.buffer_pool_used", "dimensions":{}},
#           {"name":"mysql.performance.com_insert", "dimensions":{}},
#           {"name":"mysql.innodb.os_log_fsyncs", "dimensions":{}},
#           {"name":"mysql.performance.com_replace_select", "dimensions":{}},
#           {"name":"mysql.performance.qcache_hits", "dimensions":{}},
#           {"name":"mysql.innodb.current_row_locks", "dimensions":{}},
#           {"name":"mysql.performance.created_tmp_tables", "dimensions":{}},
#           {"name":"mysql.performance.created_tmp_disk_tables", "dimensions":{}},
#           {"name":"mysql.innodb.row_lock_waits", "dimensions":{}},
#           {"name":"mysql.performance.com_delete_multi", "dimensions":{}},
#           {"name":"mysql.performance.slow_queries", "dimensions":{}},
#           {"name":"mysql.innodb.data_reads", "dimensions":{}},
#           {"name":"mysql.performance.com_update", "dimensions":{}},
#           {"name":"mysql.innodb.row_lock_time", "dimensions":{}},
#           {"name":"mysql.performance.table_locks_waited", "dimensions":{}},
#           {"name":"mysql.net.max_connections", "dimensions":{}},
#           {"name":"mysql.innodb.mutex_spin_waits", "dimensions":{}},
#           {"name":"mysql.performance.com_insert_select", "dimensions":{}},
#           {"name":"mysql.performance.open_files", "dimensions":{}},
#           {"name":"mysql.performance.threads_connected", "dimensions":{}},
#           {"name":"mysql.performance.com_select", "dimensions":{}},
#           #{"name":"rabbitmq.node.fd_used", "dimensions":{}},
#           #{"name":"rabbitmq.node.mem_used", "dimensions":{}},
#           #{"name":"rabbitmq.node.run_queue", "dimensions":{}},
#           #{"name":"rabbitmq.node.sockets_used", "dimensions":{}},
#           ]

# list of hostnames to collect
hostnames = ["ipc1-ccp-c1-m1-mgmt",
             "ipc1-ccp-c1-m2-mgmt",
             "ipc1-ccp-c1-m3-mgmt"]

start_time = "2015-09-29T12:00:00.000Z"
end_time = "2015-10-30T23:59:59.000Z"

ks_url = "http://10.241.67.7:35357/v3/"
ks_user = "admin"
ks_pass = "admin"
ks_project_name = "admin"
ks_project_domain = "default"
mon_url = "http://10.241.67.7:8070/v2.0/"

# get all metrics (in json) and collect into single list, write to file
ksclient = ksclient.KSClient(auth_url=ks_url,
                             username=ks_user,
                             password=ks_pass,
                             project_name=ks_project_name,
                             project_domain_name=ks_project_domain)
mon_client = client.Client("2_0", mon_url, token=ksclient.token)


rabbit_exchange = ["rabbitmq.exchange.messages.published_count",
                   "rabbitmq.exchange.messages.published_rate",
                   "rabbitmq.exchange.messages.received_count",
                   "rabbitmq.exchange.messages.received_rate"]

rabbit_nodes = ["rabbitmq.node.fd_used",
                "rabbitmq.node.mem_used",
                "rabbitmq.node.run_queue",
                "rabbitmq.node.sockets_used"]

rabbit_queues = ["rabbitmq.queue.active_consumers",
                 "rabbitmq.queue.consumers",
                 "rabbitmq.queue.memory",
                 "rabbitmq.queue.messages",
                 "rabbitmq.queue.messages.ack_count",
                 "rabbitmq.queue.messages.ack_rate",
                 "rabbitmq.queue.messages.deliver_count",
                 "rabbitmq.queue.messages.deliver_get_count",
                 "rabbitmq.queue.messages.deliver_get_rate",
                 "rabbitmq.queue.messages.deliver_rate",
                 "rabbitmq.queue.messages.publish_count",
                 "rabbitmq.queue.messages.publish_rate",
                 "rabbitmq.queue.messages.rate",
                 "rabbitmq.queue.messages.ready",
                 "rabbitmq.queue.messages.ready_rate",
                 "rabbitmq.queue.messages.redeliver_count",
                 "rabbitmq.queue.messages.redeliver_rate",
                 "rabbitmq.queue.messages.unacknowledged",
                 "rabbitmq.queue.messages.unacknowledged_rate"]


metrics = []

rabbit_names = {'exchange': [],
                'queue': [],
                'node': []}

for rabbit in mon_client.metrics.list(name="rabbitmq.exchange.messages.published_rate"):
    try:
        rabbit_names['exchange'].append(rabbit['dimensions']['exchange'])
    except Exception:
        pass

for rabbit in mon_client.metrics.list(name="rabbitmq.node.fd_used"):
    try:
        rabbit_names['node'].append(rabbit['dimensions']['node'])
    except Exception:
        pass

for rabbit in mon_client.metrics.list(name="rabbitmq.queue.messages.redeliver_rate"):
    try:
        rabbit_names['queue'].append(rabbit['dimensions']['queue'])
    except Exception:
        pass

#with open('rabbit_exchange.json', 'w') as f:
#    for name in rabbit_names['exchange']:
#        measurement_list = []
#        dim = {'exchange': name, 'hostname': hostnames[0]}
#        for metric in rabbit_exchange:
#            print "{} -> {}".format(name, metric)
#            measure = mon_client.metrics.list_measurements(name=metric,
#                                                           dimensions=dim,
#                                                           start_time=start_time,
#                                                           end_time=end_time)
#            measurement_list.append(measure)
#        f.write(json.dumps(measurement_list))

with open('rabbit_nodes.json', 'w') as f:
    for name in rabbit_names['node']:
        measurement_list = []
        dim = {'node': name, 'hostname': hostnames[0]}
        for metric in rabbit_nodes:
            print "{} -> {}".format(name, metric)
            measure = mon_client.metrics.list_measurements(name=metric,
                                                           dimensions=dim,
                                                           start_time=start_time,
                                                           end_time=end_time)
            measurement_list.append(measure)
        f.write(json.dumps(measurement_list))

with open('rabbit_queues.json', 'w') as f:
    for name in rabbit_names['queue']:
        measurement_list = []
        dim = {'queue': name, 'hostname': hostnames[0]}
        for metric in rabbit_queues:
            print "{} -> {}".format(name, metric)
            measure = mon_client.metrics.list_measurements(name=metric,
                                                           dimensions=dim,
                                                           start_time=start_time,
                                                           end_time=end_time)
            measurement_list.append(measure)
        f.write(json.dumps(measurement_list))

import sys
sys.exit()
measurement_list = []
for metric in metrics:
    for hostname in hostnames:
        metric['dimensions']['hostname'] = hostname
        measure = mon_client.metrics.list_measurements(name=metric['name'],
                                                       dimensions=metric['dimensions'],
                                                       start_time=start_time,
                                                       end_time=end_time)
        measurement_list.append(measure)
print(json.dumps(measurement_list))
