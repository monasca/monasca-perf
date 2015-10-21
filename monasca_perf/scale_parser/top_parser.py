import re
import sys

monitoring = ['mon-api',
              'mon-per+',
              'mon-age+',
              'mon-not+',
              'storm',
              'kafka',
              'zookeep+',
              'dbadmin',
              'mysql']

logging = ['rabbitmq',
           'elastic+',
           'logstash',
           'beaver']

openstack = ['nova',
             'neutron',
             'cinder',
             'glance',
             'ceilome+',
             'swift',
             'heat',
             'horizon+']

hos = ['haproxy',
       'memcache',
       'opscon']

watched_processes = monitoring + logging + openstack + hos

cpu = {'user': [],
       'sys': [],
       'idle': [],
       'wait': []}

memory = {'free': [],
          'buffers': [],
          'cache': []}

# samples_per_average = int(sys.argv[2])
samples_per_average = 60
system_memory = 125


def avg(l):
    if len(l) == 0:
        return 0
    return round((sum(l) / len(l)), 2)


def build_avg_set(l):
    return [avg(l[x:x + samples_per_average]) for x in xrange(0, len(l), samples_per_average)]


def min_free(node):
    free = min(node['system_mem']['free'])
    buffers = min(node['system_mem']['buffers'])
    cache = min(node['system_mem']['cache'])
    return free + buffers + cache


def max_used(node):
    return system_memory - min_free(node)


def normalize_memory(mem):
    result = re.match("(.*?)m", mem)
    if result:
        return float(result.group(1)) * 1024

    result = re.match("(.*?)g", mem)
    if result:
        return float(result.group(1)) * 1024 * 1024

    result = re.match("(.*?)t", mem)
    if result:
        return float(result.group(1)) * 1024 * 1024 * 1024

    return mem


def aggregate_process_data(node_data, process_data):
    for k, v in process_data.iteritems():
        data = node_data.get(k, {'cpu': [], 'mem': []})
        data['cpu'].append(sum(v['cpu']))
        data['mem'].append(sum(v['mem']))
        node_data[k] = data


def parse_top_file(path):
    node_data = {'system_cpu': {'user': [],
                                'sys': [],
                                'idle': [],
                                'wait': []},
                 'system_mem': {'free': [],
                                'buffers': [],
                                'cache': []}}

    process_data = {}
    with open(path, 'r') as f:
        for line in f:
            line = line.rstrip()

            if not line:
                continue

            result = re.match("top -", line)
            if result:
                aggregate_process_data(node_data, process_data)
                process_data = {}

            result = re.match("Tasks:", line)
            if result:
                continue

            result = re.match("%Cpu.*?(\d+.\d+) us.*?(\d+.\d+) sy.*?(\d+.\d+) id.*?(\d+.\d+) wa", line)
            if result:
                node_data['system_cpu']['user'].append(float(result.group(1)))
                node_data['system_cpu']['sys'].append(float(result.group(2)))
                node_data['system_cpu']['idle'].append(float(result.group(3)))
                node_data['system_cpu']['wait'].append(float(result.group(4)))
                continue

            result = re.match("KiB Mem:.*?(\d+) free.*?(\d+) buffers", line)
            if result:
                node_data['system_mem']['free'].append(float(result.group(1)) / 1024 / 1024)
                node_data['system_mem']['buffers'].append(float(result.group(2)) / 1024 / 1024)
                continue

            result = re.match("KiB Swap:.*?(\d+) cached", line)
            if result:
                node_data['system_mem']['cache'].append(float(result.group(1)) / 1024 / 1024)
                continue

            result = re.match("^\s*\d", line)
            if result:
                pid, user, _, _, virt, res, shr, _, cpup, mem, _, cmd = line.split()

                if user in watched_processes:
                    res = normalize_memory(res)
                    data_points = process_data.get(user, {'cpu': [], 'mem': []})
                    data_points['cpu'].append(float(cpup))
                    data_points['mem'].append(float(res))
                    process_data[user] = data_points
        else:
            aggregate_process_data(node_data, process_data)

    return node_data


def process_group_report(processes, node1, node2, node3):
    print("")
    print("{:<10}| {:^8} | {:^8} | {:^8}".format("CPU", "Node 1", "Node 2", "Node 3"))
    print("------------------------------------------")
    total_cpu = {'node1': [], 'node2': [], 'node3': []}
    for k in processes:
        n1 = avg(node1[k]['cpu'])
        n2 = avg(node2[k]['cpu'])
        n3 = avg(node3[k]['cpu'])
        total_cpu['node1'].append(n1)
        total_cpu['node2'].append(n2)
        total_cpu['node3'].append(n3)
        print("{:<10}| {:>8.2f} | {:>8.2f} | {:>8.2f}".format(k, n1, n2, n3))

    print("------------------------------------------")
    print("{:<10}| {:>8.2f} | {:>8.2f} | {:8.2f}"
          .format('total',
                  sum(total_cpu['node1']),
                  sum(total_cpu['node2']),
                  sum(total_cpu['node3'])))

    print("")
    print("{:<10}| {:^8} | {:^8} | {:^8}".format("MEM", "Node 1", "Node 2", "Node 3"))
    print("------------------------------------------")
    total_mem = {'node1': [], 'node2': [], 'node3': []}
    for k in processes:
        total_mem['node1'].append(max(node1[k]['mem']))
        total_mem['node2'].append(max(node2[k]['mem']))
        total_mem['node3'].append(max(node3[k]['mem']))
        print("{:<10}| {:>8.2f} | {:>8.2f} | {:>8.2f}"
              .format(k,
                      max(node1[k]['mem']) / 1024,
                      max(node2[k]['mem']) / 1024,
                      max(node3[k]['mem']) / 1024))

    print("------------------------------------------")
    print("{:<10}| {:>8.2f} | {:>8.2f} | {:8.2f}"
          .format('total',
                  sum(total_mem['node1']) / 1024,
                  sum(total_mem['node2']) / 1024,
                  sum(total_mem['node3']) / 1024))


def generate_report(node1, node2, node3):
    print("{:<10}| {:^8} | {:^8} | {:^8}".format("SYSTEM", "Node 1", "Node 2", "Node 3"))
    print("------------------------------------------")
    print("{:<10}| {:>8.2f} | {:>8.2f} | {:>8.2f}".format("idle %",
          avg(node1['system_cpu']['idle']),
          avg(node2['system_cpu']['idle']),
          avg(node3['system_cpu']['idle'])))

    print("{:<10}| {:>8.2f} | {:>8.2f} | {:>8.2f}".format("min free",
          min_free(node1),
          min_free(node2),
          min_free(node3)))
    print("{:<10}| {:>8.2f} | {:>8.2f} | {:>8.2f}".format("max used",
          max_used(node1),
          max_used(node2),
          max_used(node3)))

    for name, group in [('Monitoring', monitoring),
                        ('Logging', logging),
                        ('Openstack', openstack),
                        ('HOS', hos)]:
        print("")
        print("-- {} ------------------".format(name))
        process_group_report(group, node1, node2, node3)


generate_report(parse_top_file(sys.argv[1] + '/node1/' + sys.argv[2] + '/system.top'),
                parse_top_file(sys.argv[1] + '/node2/' + sys.argv[2] + '/system.top'),
                parse_top_file(sys.argv[1] + '/node3/' + sys.argv[2] + '/system.top'))


# print "mysql: {}".format(build_avg_set(processes['mysql']['cpu']))
# print "kafka: {}".format(build_avg_set(processes['kafka']['cpu']))
# print "mon-per+: {}".format(build_avg_set(processes['mon-per+']['cpu']))
# print "mon-api: {}".format(build_avg_set(processes['mon-api']['cpu']))
