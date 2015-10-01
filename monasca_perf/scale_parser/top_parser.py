import collections
import re
import sys

data = collections.OrderedDict()

timestamp = ""

user_match = ['kafka', 'mysql', 'dbadmin', 'zookeep+', 'mon-api',
              'mon-per+', 'mon-not+', 'mon-age+']

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

        # result = re.match("top - (.*?) up", line)
        # if result:
        #    timestamp = result.group(1)
        # else:
        #   pid, user, _, _, virt, res, shr, _, cpu, mem, _, cmd = line.split()

            # if user in user_match:
            #    match = [user, virt, res, cpu, mem, cmd]
            #    print match


def avg(l):
    return round((sum(l) / len(l)), 2)


def build_avg_set(l):
    return [avg(l[x:x + samples_per_average]) for x in xrange(0, len(l), samples_per_average)]

print("-- CPU ------------------------------")
print("user: {}".format(build_avg_set(cpu['user'])))
print("sys:  {}".format(build_avg_set(cpu['sys'])))
print("idle: {}".format(build_avg_set(cpu['idle'])))
print("wait: {}".format(build_avg_set(cpu['wait'])))

free = build_avg_set(memory['free'])
buffers = build_avg_set(memory['buffers'])
cache = build_avg_set(memory['cache'])

used = []
for f, b, c in zip(free, buffers, cache):
    used.append(round((total_mem - (f + b + c)), 2))

total_free = []
for f, b, c in zip(free, buffers, cache):
    total_free.append(round((f + b + c), 2))

print("-- MEM ------------------------------")
print("used: {}".format(used))
print("total_free: {}".format(total_free))
print("free:  {}".format(build_avg_set(memory['free'])))
print("buffers: {}".format(build_avg_set(memory['buffers'])))
print("cache: {}".format(build_avg_set(memory['cache'])))
