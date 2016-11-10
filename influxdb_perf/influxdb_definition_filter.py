import datetime
import hashlib
import random
import os
import string
import sys
import time
import uuid

from influxdb import InfluxDBClient
from multiprocessing import Pool

""" influxdb_definition_filler
    This will simulate a number of days worth of metric definition history.
"""

# Clear the current metrics from the DB for testing
CLEAR_METRICS = True

# Add metrics every 30 seconds for the full measurement load (false, send only one metric per hour)
FULL_MEASUREMENTS = False

# Total vms active at one time
TOTAL_ACTIVE_VMS = 800

# Number of new vms per hour.
NEW_VMS_PER_HOUR = 80

# Number of VMs less than probation time per hour (i.e. remove new vms after a single report)
VMS_BELOW_PROBATION = 8

# start day
BASE_TIMESTAMP = datetime.datetime.utcnow() - datetime.timedelta(days=45)
# number of days to fill
DAYS_TO_FILL = 1  # 4

CONN_INFO = {'user': 'dbadmin',
             'password': 'password'
             }

# Tenant id to report metrics under
TENANT_ID = "98737d81752f4ebcba8e576a05dc487b"
# Region in which to report metrics
REGION = "Region 1"


# Number of different tenants to create vms under
TOTAL_VM_TENANTS = 256

# number of definitions to store in memory before writing to influxdb
LOCAL_STORAGE_MAX = 1000000

MEASUREMENTS_FILENAME = './measurements.txt'

def_id_set = set()
dim_id_set = set()
def_dim_id_set = set()
def_list = []
dims_list = []
def_dims_list = []

measurement_process_id = 0
TOTAL_MEASUREMENT_PROCESSES = 5

next_hostname_id = 1
measurements_per_hour = 120 if FULL_MEASUREMENTS else 1

ID_SIZE = 20

DATABASE_NAME = 'monasca'
client = InfluxDBClient('localhost', 8086, 'root', 'root', DATABASE_NAME)

print "Creating database: {}...".format(DATABASE_NAME)
client.create_database(DATABASE_NAME)

db_user = 'admin'
db_user_password = 'my_secret_password'

print "Switch user: {}".format(db_user)
client.switch_user(db_user, db_user_password)


class vmSimulator(object):
    disks = ['sda', 'sdb', 'sdc']
    vswitches = ['vs1', 'vs2', 'vs3']
    network_devices = ['tap1']
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
                    "vm.ping_status"]
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
                         "vm.io.write_ops_sec"]
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
            self.metric_ids.add((metric_id, name, tenant_id, REGION,
                                 dimensions['cloud_name'], dimensions['cluster'],
                                 dimensions['service'], dimensions['resource_id'],
                                 dimensions['zone'], dimensions['component'],
                                 dimensions['hostname'], dimensions['lifespan']))

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
            self.metric_ids.add((metric_id, name, tenant_id, REGION,
                                 dimensions['cloud_name'], dimensions['cluster'],
                                 dimensions['service'], dimensions['resource_id'],
                                 dimensions['zone'], dimensions['component'],
                                 dimensions['hostname'], dimensions['lifespan']))
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
            self.metric_ids.add((metric_id, name, tenant_id, REGION,
                                 dimensions['cloud_name'], dimensions['cluster'],
                                 dimensions['service'], dimensions['resource_id'],
                                 dimensions['zone'], dimensions['component'],
                                 dimensions['hostname'], dimensions['lifespan']))

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
            self.metric_ids.add((metric_id, name, tenant_id, REGION,
                                 dimensions['cloud_name'], dimensions['cluster'],
                                 dimensions['service'], dimensions['resource_id'],
                                 dimensions['zone'], dimensions['component'],
                                 dimensions['hostname'], dimensions['lifespan']))

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
            self.metric_ids.add((metric_id, name, tenant_id, REGION,
                                 dimensions['cloud_name'], dimensions['cluster'],
                                 dimensions['service'], dimensions['resource_id'],
                                 dimensions['zone'], dimensions['component'],
                                 dimensions['hostname'], dimensions['lifespan']))

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

    def create_measurements(self):
        if self.current_cycle >= self.lifespan_cycles:
            return []

        meas_list = []
        for metric_id in self.get_metric_ids(self.current_cycle):
            value = str(random.randint(0, 1000000))
            meas_list.append('{},cloud_name={},cluster={},service={},resource_id={},zone={},component={},hostname={},lifespan={} value={},metric_id="{}",tenant_id="{}",region="{}"'.format(
                metric_id[1], metric_id[4], metric_id[5], metric_id[6], metric_id[7], metric_id[8],
                metric_id[9], metric_id[10], metric_id[11], value, metric_id[0], metric_id[2],
                metric_id[3]))
        self.current_cycle += 1
        return meas_list

    @staticmethod
    def get_total_metric_defs():
        base_defs = len(vmSimulator.metric_names)
        disk_agg_defs = len(vmSimulator.disk_agg_metric_names)
        disk_defs = len(vmSimulator.disk_metric_names) * len(vmSimulator.disks)
        network_defs = len(vmSimulator.network_metric_names) * len(vmSimulator.network_devices)
        vswitch_defs = len(vmSimulator.vswitch_metric_names) * len(vmSimulator.vswitches)

        return base_defs + disk_agg_defs + disk_defs + network_defs + vswitch_defs


def add_measurement_batch(vm_list, filename=MEASUREMENTS_FILENAME):
    new_measurements = []
    for vm in vm_list:
        new_measurements.extend(vm.create_measurements())
        if len(new_measurements) >= 5000:
            flush_measurement_data(new_measurements, filename)
            new_measurements = []
    flush_measurement_data(new_measurements, filename)


def add_full_definition(name, dimensions, tenant_id='tenant_1', region='region_1',
                        def_dim_id=None, definition_id=None, dimension_set_id=None):
    if definition_id is None:
        id_hash = hashlib.sha1()
        id_hash.update(str(name) + str(tenant_id) + str(region))
        definition_id = id_hash.hexdigest()

    if definition_id not in def_id_set:
        def_list.append(','.join([definition_id, name, tenant_id, region]))
        def_id_set.add(definition_id)

    if dimension_set_id is None:
        id_hash = hashlib.sha1()
        id_hash.update(','.join([str(key) + '=' + str(dimensions[key]) for key in dimensions.keys()]))
        dimension_set_id = id_hash.hexdigest()

    if dimension_set_id not in dim_id_set:
        dims_list.append(get_dimension_set_str(dimensions, dimension_set_id))
        dim_id_set.add(dimension_set_id)

    if def_dim_id is None:
        id_hash = hashlib.sha1()
        id_hash.update(str(definition_id) + str(dimension_set_id))
        def_dim_id = id_hash.hexdigest()

    if def_dim_id not in def_dim_id_set:
        def_dims_list.append(','.join([def_dim_id, definition_id, dimension_set_id]))
        def_dim_id_set.add(def_dim_id)

    return def_dim_id


def get_dimension_set_str(dimensions, dimension_set_id):
    dim_data = []
    for key in dimensions.iterkeys():
        dim_data.append(','.join([dimension_set_id, key, dimensions[key]]))

    return '\n'.join(dim_data)


def set_dimension_values(active_dimensions, base_dimensions, day, hour, definition):
    for key in base_dimensions.keys():
        active_dimensions[key] = base_dimensions[key].format(day=day,
                                                             hour=hour,
                                                             definition=definition)


def flush_measurement_data(meas_list, filename):
    print "write_points batch size: {}".format(len(meas_list))
    client.write_points(meas_list, batch_size=len(meas_list),
                        time_precision='ms', protocol='line')


def id_generator(size=32, chars=string.hexdigits):
    return ''.join(random.choice(chars) for _ in range(size))


def fill_metrics(base_timestamp, days_to_fill, new_vms_per_hour, vms_below_probation):
    measurement_process_pool = Pool(TOTAL_MEASUREMENT_PROCESSES)

    vm_tenant_ids = [id_generator(ID_SIZE) for _ in range(TOTAL_VM_TENANTS)]

    seconds_per_cycle = 30 if FULL_MEASUREMENTS else 3600  # 30 sec or 1 hour
    cycles_per_hour = 3600 / seconds_per_cycle
    churn_lifespan = (TOTAL_ACTIVE_VMS / NEW_VMS_PER_HOUR) * cycles_per_hour
    available_lifespan = days_to_fill * 24 * cycles_per_hour

    standard_lifespan = min(churn_lifespan, available_lifespan)

    start_time = time.time()
    for x in xrange(days_to_fill):
        for y in xrange(24):
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
            global total_num_meas
            print "add measurement batch for process id = {}".format(measurement_process_id)
            measurement_process_pool.apply_async(add_measurement_batch,
                                                 args=(active_vms,
                                                       MEASUREMENTS_FILENAME +
                                                       str(measurement_process_id,)))
            measurement_process_id += 1

    print("Waiting for measurement process pool to close")
    measurement_process_pool.close()
    measurement_process_pool.join()

    total_time_delta = time.time() - start_time
    print "total time delta = {}".format(total_time_delta)


def influx_db_filler():
    print("Creating metric history for {} days".format(DAYS_TO_FILL))
    fill_metrics(BASE_TIMESTAMP, DAYS_TO_FILL, NEW_VMS_PER_HOUR, VMS_BELOW_PROBATION)
    print("Checking if data arrived...")
    print("DefinitionDimensions")
    query = 'SHOW series limit 1'
    result = client.query(query)
    print "Result = {}".format(result)
    print('Finished loading InfluxDB')


if __name__ == "__main__":
    sys.exit(influx_db_filler())
