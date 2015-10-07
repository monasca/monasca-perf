import collections
import re
import sys

processes = collections.OrderedDict()
processes['mon-api'] = {'cpu': [], 'mem': []}
processes['mon-per+'] = {'cpu': [], 'mem': []}
processes['mon-age+'] = {'cpu': [], 'mem': []}
processes['mon-not+'] = {'cpu': [], 'mem': []}
processes['storm'] = {'cpu': [], 'mem': []}
processes['kafka'] = {'cpu': [], 'mem': []}
processes['zookeep+'] = {'cpu': [], 'mem': []}
processes['dbadmin'] = {'cpu': [], 'mem': []}
processes['mysql'] = {'cpu': [], 'mem': []}

match_processes = processes.keys()

cpu = {'user': [],
       'sys': [],
       'idle': [],
       'wait': []}

memory = {'free': [],
          'buffers': [],
          'cache': []}

samples_per_average = int(sys.argv[2])
total_mem = 125


def aggregate_process_data(process_data):
    for k, v in process_data.iteritems():
        processes[k]['cpu'].append(sum(v['cpu']))
        processes[k]['mem'].append(sum(v['mem']))


def parse_top_file(path):
    process_data = {}
    with open(path, 'r') as f:
        for line in f:
            line = line.rstrip()

            if not line:
                continue

            result = re.match("top -", line)
            if result:
                aggregate_process_data(process_data)
                process_data = {}

            result = re.match("Tasks:", line)
            if result:
                continue

            result = re.match("%Cpu.*?(\d+.\d+) us.*?(\d+.\d+) sy.*?(\d+.\d+) id.*?(\d+.\d+) wa", line)
            if result:
                cpu['user'].append(float(result.group(1)))
                cpu['sys'].append(float(result.group(2)))
                cpu['idle'].append(float(result.group(3)))
                cpu['wait'].append(float(result.group(4)))
                continue

            result = re.match("KiB Mem:.*?(\d+) free.*?(\d+) buffers", line)
            if result:
                memory['free'].append(float(result.group(1)) / 1024 / 1024)
                memory['buffers'].append(float(result.group(2)) / 1024 / 1024)
                continue

            result = re.match("KiB Swap:.*?(\d+) cached", line)
            if result:
                memory['cache'].append(float(result.group(1)) / 1024 / 1024)
                continue

            result = re.match("^\s*\d", line)
            if result:
                pid, user, _, _, virt, res, shr, _, cpup, mem, _, cmd = line.split()

                if user == "dbadmin":
                    result = re.match(".*vertica$", cmd)
                    if not result:
                        continue
                if user == "mysql":
                    result = re.match(".*mysqld$", cmd)
                    if not result:
                        continue

                if user in match_processes:
                    result = re.match("(.*?)g", res)
                    if result:
                        res = float(result.group(1)) * 1024 * 1024
                    data_points = process_data.get(user, {'cpu': [], 'mem': []})
                    data_points['cpu'].append(float(cpup))
                    data_points['mem'].append(float(res))
                    process_data[user] = data_points
        else:
            aggregate_process_data(process_data)


def avg(l):
    if len(l) == 0:
        return 0
    return round((sum(l) / len(l)), 2)


def build_avg_set(l):
    return [avg(l[x:x + samples_per_average]) for x in xrange(0, len(l), samples_per_average)]

parse_top_file(sys.argv[1])

print("-- CPU ------------------------------")
print("user: {}%".format(avg(cpu['user'])))
print("sys:  {}%".format(avg(cpu['sys'])))
print("idle: {}%".format(avg(cpu['idle'])))
print("wait: {}%".format(avg(cpu['wait'])))

free = round(min(memory['free']), 2)
buffers = round(min(memory['buffers']), 2)
cache = round(min(memory['cache']), 2)
used = round(total_mem - (free + buffers + cache), 2)
total_free = round(free + buffers + cache, 2)

print("-- MEM ------------------------------")
print("max used: {}GB".format(used))
print("min total_free: {}GB".format(total_free))
print("min free:  {}GB".format(free))
print("min buffers: {}GB".format(buffers))
print("min cache: {}GB".format(cache))

print("")
print("{:<10}| {:^8} | {:^8}".format("process", "cpu", "mem"))
print("--------------------------------------")
total_cpu = []
total_mem = []
for k, v in processes.iteritems():
    total_cpu.append(avg(v['cpu']))
    total_mem.append(max(v['mem']) / 1024)
    print("{:<10}| {:>8.2f} | {:>8.2f}MB".format(k, avg(v['cpu']), round(max(v['mem']) / 1024, 2)))
print("--------------------------------------")
print("{:<10}| {:>8.2f} | {:>8.2f}MB".format('total', sum(total_cpu), sum(total_mem)))


# print "mysql: {}".format(build_avg_set(processes['mysql']['cpu']))
# print "kafka: {}".format(build_avg_set(processes['kafka']['cpu']))
# print "mon-per+: {}".format(build_avg_set(processes['mon-per+']['cpu']))
# print "mon-api: {}".format(build_avg_set(processes['mon-api']['cpu']))
