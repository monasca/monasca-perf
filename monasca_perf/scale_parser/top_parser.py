import collections
import re
import sys

data = collections.OrderedDict()

timestamp = ""

processes = {'kafka': {'cpu': [], 'mem': []},
             'mysql': {'cpu': [], 'mem': []},
             'dbadmin': {'cpu': [], 'mem': []},
             'zookeep+': {'cpu': [], 'mem': []},
             'mon-api': {'cpu': [], 'mem': []},
             'mon-per+': {'cpu': [], 'mem': []}}


# 'mon-not+': [],
# 'mon-age+': []]

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

with open(sys.argv[1], 'r') as f:
    for line in f:
        line = line.rstrip()

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

        if not line:
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
                processes[user]['cpu'].append(float(cpup))
                processes[user]['mem'].append(float(res))


def avg(l):
    if len(l) == 0:
        return 0
    return round((sum(l) / len(l)), 2)


def build_avg_set(l):
    return [avg(l[x:x + samples_per_average]) for x in xrange(0, len(l), samples_per_average)]

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
print("{:<10}| {:^10}| {:^10}".format("process", "cpu", "mem"))
print("--------------------------------------")
for k, v in processes.iteritems():
    print("{:<10}| {:^10}| {:^10}MB".format(k, avg(v['cpu']), round(max(v['mem']) / 1024, 2)))
