import re

x = "%Cpu(s):  7.6 us,  6.2 sy,  1.3 ni, 81.4 id,  3.3 wa,  0.2 hi,  0.0 si,  0.0 st"

x = "KiB Mem:  13191220+total, 12997487+used,  1937336 free,  1315192 buffers"
y = "KiB Swap:        0 total,        0 used,        0 free. 63505052 cached Mem"

result = re.match("KiB Mem:.*?(\d+) free.*?(\d+) buffers", x)

print result
print result.group(1)
print result.group(2)

result = re.match("KiB Swap:.*?(\d+) cached", y)

print result
print result.group(1)

x = "47163 dbadmin   20   0 15.816g 3.311g  40716 S   0.0  2.6 852:59.74 vertica"

result = re.match(".*vertica$", x)
print result
