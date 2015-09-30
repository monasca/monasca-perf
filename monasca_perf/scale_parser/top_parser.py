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

working_set = []

samples_per_average = int(sys.argv[2])

with open(sys.argv[1], 'r') as f:
    for line in f:
        line = line.rstrip()

        result = re.match("Tasks:", line)
        if result:
            continue

        result = re.match("%Cpu.*?(\d+.\d+) us.*?(\d+.\d+) sy.*?(\d+.\d+) id.*?(\d+.\d+) wa", line)
        if result:
            cpu['user'].append(result.group(1))
            cpu['sys'].append(result.group(2))
            cpu['idle'].append(result.group(3))
            cpu['wait'].append(result.group(4))
            continue

        result = re.match("KiB Mem:.*?(\d+) free.*?(\d+) buffers", line)
        if result:
            memory['free'].append(result.group(1))
            memory['buffers'].append(result.group(2))
            continue

        result = re.match("KiB Swap:.*?(\d+) cached", line)
        if result:
            memory['cache'].append(result.group(1))
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


print cpu

print "----------------------------------"

print memory
