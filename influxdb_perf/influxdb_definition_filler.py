import datetime
import gc
import random
import requests
import string
import sys
import time
import uuid

from multiprocessing import Pool

""" vertica_definition_filler
    This will simulate a number of days worth of metric definition history.
"""

# Add metrics every 30 seconds for the full measurement load (false, send only one metric per hour)
FULL_MEASUREMENTS = True
# number of days to fill
DAYS_TO_FILL = 1

# Total vms active at one time
TOTAL_ACTIVE_VMS = 1000

# Number of new vms per hour.
NEW_VMS_PER_HOUR = 100

total_measurement_processes = 5

BATCH_SIZE = 5000

NUMBER_OF_HOURS_PER_DAY = 24
# Number of VMs less than probation time per hour (i.e. remove new vms after a single report)
VMS_BELOW_PROBATION = 0

# start day
BASE_TIMESTAMP = datetime.datetime.utcnow() - datetime.timedelta(days=45)

CONN_INFO = {'user': 'dbadmin',
             'password': 'password'
             }

# Tenant id to report metrics under
TENANT_ID = "98737d81752f4ebcba8e576a05dc487b"
# Region in which to report metrics
REGION = "Region_1"

# Database name for influx client
DATABASE_NAME = 'monasca'

url = 'http://localhost:8086/query'
param = 'q=CREATE DATABASE monasca'
requests.get(url=url, params=param)

# Number of different tenants to create vms under
TOTAL_VM_TENANTS = 256

MEASUREMENTS_FILENAME = '/tmp/measurements.txt'

measurement_process_id = 0

next_hostname_id = 1

ID_SIZE = 20


class vmSimulator(object):
    disks = ['sda', 'sdb', 'sdc', 'sdd']
    vswitches = ['vs1', 'vs2', 'vs3']
    network_devices = ['tap1', 'tap2']
    # 420 with 4 disk 3 vswitch 2 network device
    # 62
    metric_names = ["cpu.time_ns",
                    "cpu.utilization_norm_perc",
                    "cpu.utilization_perc",
                    "host_alive_status",
                    "instance",
                    "mem.free_mb",
                    "mem.free_perc",
                    "memory",
                    "mem.swap_used_mb",
                    "mem.total_mb",
                    "mem.used_mb",
                    "ping_status",
                    "vcpus",
                    "vm.cpu.time_ns",
                    "vm.cpu.utilization_norm_perc",
                    "vm.cpu.utilization_perc",
                    "vm.host_alive_status",
                    "vm.mem.free_mb",
                    "vm.mem.free_perc",
                    "vm.mem.resident_mb",
                    "vm.mem.swap_used_mb",
                    "vm.mem.total_mb",
                    "vm.mem.used_mb",
                    "vm.ping_status",
                    "cpu.frequency_mhz",
                    "cpu.idle_perc",
                    "cpu.idle_time",
                    "cpu.percent",
                    "cpu.stolen_perc",
                    "cpu.system_perc",
                    "cpu.system_time",
                    "cpu.total_logical_cores",
                    "cpu.user_perc",
                    "cpu.user_time",
                    "cpu.wait_perc",
                    "cpu.wait_time",
                    "mem.swap_free_mb",
                    "mem.swap_free_perc",
                    "mem.swap_total_mb",
                    "mem.usable_mb",
                    "mem.usable_perc",
                    "mem.used_buffers",
                    "mem.used_cache",
                    "vm.cpu.frequency_mhz",
                    "vm.cpu.idle_perc",
                    "vm.cpu.idle_time",
                    "vm.cpu.percent",
                    "vm.cpu.stolen_perc",
                    "vm.cpu.system_perc",
                    "vm.cpu.system_time",
                    "vm.cpu.total_logical_cores",
                    "vm.cpu.user_perc",
                    "vm.cpu.user_time",
                    "vm.cpu.wait_perc",
                    "vm.cpu.wait_time",
                    "vm.mem.swap_free_mb",
                    "vm.mem.swap_free_perc",
                    "vm.mem.swap_total_mb",
                    "vm.mem.usable_mb",
                    "vm.mem.usable_perc",
                    "vm.mem.used_buffers",
                    "vm.mem.used_cache"]
    # 12
    process_metric_names = ["process.cpu_perc",
                            "process.io.read_count",
                            "process.io.read_kbytes",
                            "process.io.write_count",
                            "process.io.write_kbytes",
                            "process.mem.rss_mbytes",
                            "process.open_file_descriptors",
                            "process.pid_count",
                            "process.thread_count",
                            "load.avg_15_min",
                            "load.avg_1_min",
                            "load.avg_5_min"]
    # 26
    disk_agg_metric_names = ["disk.allocation_total",
                             "disk.capacity_total",
                             "disk.physical_total",
                             "io.errors_total",
                             "io.errors_total_sec",
                             "io.read_bytes_total",
                             "io.read_bytes_total_sec",
                             "io.read_ops_total",
                             "io.read_ops_total_sec",
                             "io.write_bytes_total",
                             "io.write_bytes_total_sec",
                             "io.write_ops_total",
                             "io.write_ops_total_sec",
                             "vm.disk.allocation_total",
                             "vm.disk.capacity_total",
                             "vm.disk.physical_total",
                             "vm.io.errors_total",
                             "vm.io.errors_total_sec",
                             "vm.io.read_bytes_total",
                             "vm.io.read_bytes_total_sec",
                             "vm.io.read_ops_total",
                             "vm.io.read_ops_total_sec",
                             "vm.io.write_bytes_total",
                             "vm.io.write_bytes_total_sec",
                             "vm.io.write_ops_total",
                             "vm.io.write_ops_total_sec"]
    # 48 * 4 = 192
    disk_metric_names = ["disk.allocation",
                         "disk.capacity",
                         "disk.ephemeral.size",
                         "disk.physical",
                         "disk.root.size",
                         "io.errors",
                         "io.errors_sec",
                         "io.read_bytes",
                         "io.read_bytes_sec",
                         "io.read_ops",
                         "io.read_ops_sec",
                         "io.write_bytes",
                         "io.write_bytes_sec",
                         "io.write_ops",
                         "io.write_ops_sec",
                         "vm.disk.allocation",
                         "vm.disk.capacity",
                         "vm.disk.physical",
                         "vm.io.errors",
                         "vm.io.errors_sec",
                         "vm.io.read_bytes",
                         "vm.io.read_bytes_sec",
                         "vm.io.read_ops",
                         "vm.io.read_ops_sec",
                         "vm.io.write_bytes",
                         "vm.io.write_bytes_sec",
                         "vm.io.write_ops",
                         "vm.io.write_ops_sec",
                         "disk.inode_used_perc",
                         "disk.space_used_perc",
                         "disk.total_space_mb",
                         "disk.total_used_space_mb",
                         "io.read_kbytes_sec",
                         "io.read_req_sec",
                         "io.read_time_sec",
                         "io.write_kbytes_sec",
                         "io.write_req_sec",
                         "io.write_time_sec",
                         "vm.disk.inode_used_perc",
                         "vm.disk.space_used_perc",
                         "vm.disk.total_space_mb",
                         "vm.disk.total_used_space_mb",
                         "vm.io.read_kbytes_sec",
                         "vm.io.read_req_sec",
                         "vm.io.read_time_sec",
                         "vm.io.write_kbytes_sec",
                         "vm.io.write_req_sec",
                         "vm.io.write_time_sec"]
    # 32 * 3 = 96
    vswitch_metric_names = ["vm.vswitch.in_bytes",
                            "vm.vswitch.in_bytes_sec",
                            "vm.vswitch.in_packets",
                            "vm.vswitch.in_packets_sec",
                            "vm.vswitch.in_dropped",
                            "vm.vswitch.in_dropped_sec",
                            "vm.vswitch.in_errors",
                            "vm.vswitch.in_errors_sec",
                            "vm.vswitch.out_bytes",
                            "vm.vswitch.out_bytes_sec",
                            "vm.vswitch.out_packets",
                            "vm.vswitch.out_packets_sec",
                            "vm.vswitch.out_dropped",
                            "vm.vswitch.out_dropped_sec",
                            "vm.vswitch.out_errors",
                            "vm.vswitch.out_errors_sec",
                            "vswitch.in_bytes",
                            "vswitch.in_bytes_sec",
                            "vswitch.in_packets",
                            "vswitch.in_packets_sec",
                            "vswitch.in_dropped",
                            "vswitch.in_dropped_sec",
                            "vswitch.in_errors",
                            "vswitch.in_errors_sec",
                            "vswitch.out_bytes",
                            "vswitch.out_bytes_sec",
                            "vswitch.out_packets",
                            "vswitch.out_packets_sec",
                            "vswitch.out_dropped",
                            "vswitch.out_dropped_sec",
                            "vswitch.out_errors",
                            "vswitch.out_errors_sec"]
    # 16*2=32
    network_metric_names = ["net.in_bytes",
                            "net.in_bytes_sec",
                            "net.in_packets",
                            "net.in_packets_sec",
                            "net.out_bytes",
                            "net.out_bytes_sec",
                            "net.out_packets",
                            "net.out_packets_sec",
                            "vm.net.in_bytes",
                            "vm.net.in_bytes_sec",
                            "vm.net.in_packets",
                            "vm.net.in_packets_sec",
                            "vm.net.out_bytes",
                            "vm.net.out_bytes_sec",
                            "vm.net.out_packets",
                            "vm.net.out_packets_sec"]

    def __init__(self, resource_id,
                 hostname='test_vm_host_1',
                 admin_tenant_id='tenant_1',
                 tenant_id='vm_tenant_1',
                 region='region_1',
                 created_timestamp=datetime.datetime.utcnow(),
                 lifespan_cycles=20,
                 seconds_per_cycle=30):
        self.resource_id = resource_id
        self.admin_tenant_id = admin_tenant_id
        self.vm_tenant_id = tenant_id
        self.region = region
        self.created_timestamp = created_timestamp

        self.seconds_per_cycle = seconds_per_cycle
        self.current_cycle = 0
        self.lifespan_cycles = lifespan_cycles

        self.metric_ids = set()
        self.report_per_cycle = 1

        self.disk_metric_ids = set()
        self.disk_report_per_cycle = 1

        self.vswitch_metric_ids = set()
        self.vswitch_report_per_cycle = 1

        self.base_dimensions = {
            "cloud_name": "test_cloud",
            "cluster": "test_cluster",
            "service": "compute",
            "resource_id": str(resource_id),
            "zone": "nova",
            "component": "vm",
            "hostname": str(hostname),
            "lifespan": str(lifespan_cycles)
        }

        self.preload_metrics()

    def preload_metrics(self):
        for name in vmSimulator.metric_names:
            dimensions = self.base_dimensions.copy()
            tenant_id = self.vm_tenant_id

            if name.startswith('vm.'):
                dimensions['tenant_id'] = self.vm_tenant_id
                tenant_id = self.admin_tenant_id

            metric_id = add_full_definition(name=name,
                                            dimensions=dimensions,
                                            tenant_id=tenant_id,
                                            region=REGION)
            self.metric_ids.add(metric_id)

        for name in vmSimulator.process_metric_names:
            dimensions = self.base_dimensions.copy()
            tenant_id = self.vm_tenant_id

            if name.startswith('vm.'):
                dimensions['tenant_id'] = self.vm_tenant_id
                tenant_id = self.admin_tenant_id

            metric_id = add_full_definition(name=name,
                                            dimensions=dimensions,
                                            tenant_id=tenant_id,
                                            region=REGION)
            self.metric_ids.add(metric_id)

        for name in vmSimulator.disk_agg_metric_names:
            dimensions = self.base_dimensions.copy()
            tenant_id = self.vm_tenant_id

            if name.startswith('vm.'):
                dimensions['tenant_id'] = self.vm_tenant_id
                tenant_id = self.admin_tenant_id

            metric_id = add_full_definition(name=name,
                                            dimensions=dimensions,
                                            tenant_id=tenant_id,
                                            region=REGION)
            self.disk_metric_ids.add(metric_id)

        for name in vmSimulator.disk_metric_names:
            for disk in vmSimulator.disks:
                dimensions = self.base_dimensions.copy()
                tenant_id = self.vm_tenant_id

                if name.startswith('vm.'):
                    dimensions['tenant_id'] = self.vm_tenant_id
                    tenant_id = self.admin_tenant_id

                dimensions['device'] = disk
                metric_id = add_full_definition(name=name,
                                                dimensions=dimensions,
                                                tenant_id=tenant_id,
                                                region=REGION)
                self.disk_metric_ids.add(metric_id)

        for name in vmSimulator.network_metric_names:
            for device in vmSimulator.network_devices:
                dimensions = self.base_dimensions.copy()
                tenant_id = self.vm_tenant_id

                if name.startswith('vm.'):
                    dimensions['tenant_id'] = self.vm_tenant_id
                    tenant_id = self.admin_tenant_id

                dimensions['device'] = device
                metric_id = add_full_definition(name=name,
                                                dimensions=dimensions,
                                                tenant_id=tenant_id,
                                                region=REGION)
                self.metric_ids.add(metric_id)

        for name in vmSimulator.vswitch_metric_names:
            for switch in vmSimulator.vswitches:
                dimensions = self.base_dimensions.copy()
                tenant_id = self.vm_tenant_id

                if name.startswith('vm.'):
                    dimensions['tenant_id'] = self.vm_tenant_id
                    tenant_id = self.admin_tenant_id

                dimensions['device'] = switch
                metric_id = add_full_definition(name=name,
                                                dimensions=dimensions,
                                                tenant_id=tenant_id,
                                                region=REGION)
                self.vswitch_metric_ids.add(metric_id)

    def get_metric_ids(self, cycle=None):
        result = set()

        if not cycle:
            result = result.union(self.metric_ids)
            result = result.union(self.disk_metric_ids)
            result = result.union(self.vswitch_metric_ids)
            return result

        if (cycle % self.report_per_cycle) == 0:
            result = result.union(self.metric_ids)

        if (cycle % self.disk_report_per_cycle) == 0:
            result = result.union(self.disk_metric_ids)

        if (cycle % self.vswitch_report_per_cycle) == 0:
            result = result.union(self.vswitch_metric_ids)

        return result

    def create_measurements(self, timestamp=None):
        if self.current_cycle >= self.lifespan_cycles:
            return []

        if timestamp is None:
            second_delta = self.seconds_per_cycle * self.current_cycle
            timestamp = self.created_timestamp + datetime.timedelta(seconds=second_delta)
            timestamp = time.mktime(timestamp.timetuple()) * 1000 + self.seconds_per_cycle

        meas_list = []
        for metric_id in self.get_metric_ids(self.current_cycle):
            value = str(random.randint(0, 1000000))
            meas_list.append(' '.join([metric_id, 'value='+str(value), str(int(timestamp))]))
        self.current_cycle += 1
        return meas_list


def add_measurement_batch(active_vms, filename=MEASUREMENTS_FILENAME):
    meas_list = []
    while True:
        new_measurements = []
        for vm in active_vms:
            new_measurements.extend(vm.create_measurements())
        if len(new_measurements) <= 0:
            break
        meas_list.extend(new_measurements)

    for i in range(0, len(meas_list), BATCH_SIZE):
        batch_list = meas_list[i:i+BATCH_SIZE]
        line_protocol = '\n'.join(batch_list)
        write_url = 'http://localhost:8086/write?db=monasca'
        requests.post(write_url, data=line_protocol)


def add_full_definition(name, dimensions, tenant_id='tenant_1', region='region_1'):
    def_dim_id = [name, "_tenant_id=\"" + tenant_id + "\"", "_region=\"" + region + "\""]
    for key, value in dimensions.iteritems():
        def_dim_id.append(key + "=" + value)

    return ','.join(def_dim_id)


def id_generator(size=32, chars=string.hexdigits):
    return ''.join(random.choice(chars) for _ in range(size))


def fill_metrics(base_timestamp, days_to_fill, new_vms_per_hour, vms_below_probation):
    measurement_process_pool = Pool(total_measurement_processes)

    vm_tenant_ids = [id_generator(ID_SIZE) for _ in range(TOTAL_VM_TENANTS)]

    seconds_per_cycle = 30 if FULL_MEASUREMENTS else 3600  # 30 sec or 1 hour
    cycles_per_hour = 3600 / seconds_per_cycle
    churn_lifespan = (TOTAL_ACTIVE_VMS / NEW_VMS_PER_HOUR) * cycles_per_hour
    available_lifespan = days_to_fill * 24 * cycles_per_hour

    standard_lifespan = min(churn_lifespan, available_lifespan)

    start_time = time.time()
    for x in xrange(days_to_fill):
        for y in xrange(NUMBER_OF_HOURS_PER_DAY):
            timestamp = base_timestamp + datetime.timedelta(days=x, hours=y)
            active_vms = []
            for z in xrange(new_vms_per_hour + vms_below_probation):
                global next_hostname_id
                resource_id = uuid.uuid4()

                if z < vms_below_probation:
                    lifespan_cycles = 1
                else:
                    lifespan_cycles = standard_lifespan

                active_vms.append(vmSimulator(resource_id=resource_id,
                                              hostname='test_' + str(next_hostname_id),
                                              admin_tenant_id=TENANT_ID,
                                              tenant_id=random.choice(vm_tenant_ids),
                                              region=REGION,
                                              created_timestamp=timestamp,
                                              lifespan_cycles=lifespan_cycles,
                                              seconds_per_cycle=seconds_per_cycle))
                next_hostname_id += 1
            global measurement_process_id
            measurement_process_pool.apply_async(add_measurement_batch,
                                                 args=(active_vms,
                                                       MEASUREMENTS_FILENAME +
                                                       str(measurement_process_id,)))
            measurement_process_id += 1

    print("Waiting for measurement process pool to close")
    measurement_process_pool.close()
    measurement_process_pool.join()

    total_time_delta = time.time() - start_time
    print("total time: " + str(total_time_delta) + " sec")


def influx_db_filler():
    collected = gc.collect()
    print("Creating metric history for {} days".format(DAYS_TO_FILL))
    fill_metrics(BASE_TIMESTAMP, DAYS_TO_FILL, NEW_VMS_PER_HOUR, VMS_BELOW_PROBATION)
    print('Finished loading InfluxDB')
    print "collected = {}".format(collected)


if __name__ == "__main__":
    sys.exit(influx_db_filler())
