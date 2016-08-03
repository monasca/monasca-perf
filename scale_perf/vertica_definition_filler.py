import cProfile
import datetime
import hashlib
import random
import string
import subprocess
import sys
import time

""" vertica_definition_filler
    This will simulate a number of days worth of metric definition history.
"""

# Clear the current metrics from the DB for testing
CLEAR_METRICS = True
# Add metrics every 30 seconds for the full measurement load (false, send only one per hour)
FULL_MEASUREMENTS = False
# Total definitions active at one time
TOTAL_ACTIVE_VMS = 8000
# Number of new metric definitions per hour
NEW_VMS_PER_HOUR = 800
# will fill starting from X days ago to present (when the script started)
NUMBER_OF_DAYS = 45

CONN_INFO = {'user': 'dbadmin',
             'password': 'password'
             }

# Tenant id to report metrics under
TENANT_ID = "a314fc7078354b8d95264b3efcbacbe8"
# Region in which to report metrics
REGION = "Region 1"

DEF_DIMS_FILENAME = '/tmp/defdims.dat'

DEFINITIONS_FILENAME = '/tmp/definitions.dat'

DIMENSIIONS_FILENAME = '/tmp/dimensions.dat'

MEASUREMENTS_FILENAME = '/tmp/measurements.dat'

DEFINITION_COPY_QUERY = "COPY MonMetrics.DefinitionDimensions(id,definition_id,dimension_set_id) FROM '{}' " \
                        "DELIMITER ',' COMMIT; " \
                        "COPY MonMetrics.Definitions(id,name,tenant_id,region) FROM '{}' " \
                        "DELIMITER ',' COMMIT; " \
                        "COPY MonMetrics.Dimensions(dimension_set_id,name,value) FROM '{}' " \
                        "DELIMITER ',' COMMIT; "
MEASUREMENT_COPY_QUERY = "COPY MonMetrics.Measurements(definition_dimensions_id,time_stamp,value) FROM '{}' " \
                         "DELIMITER ',' COMMIT; "

def_id_set = set()
dim_id_set = set()
def_dim_id_set = set()
def_list = []
dims_list = []
def_dims_list = []
meas_list = []

next_resource_id = 1
measurements_per_hour = 120 if FULL_MEASUREMENTS else 1

ID_SIZE = 20
TOTAL_VM_TENANTS = 256

LOCAL_STORAGE_MAX = 100000


class vmSimulator(object):
    disks = ['sda', 'sdb', 'sdc']
    vswitches = ['vs1', 'vs2', 'vs3']
    network_devices = ['tap1']
    metric_names = ["cpu.time_ns",
                    "cpu.utilization_norm_perc",
                    "cpu.utilization_perc",
                    "disk.allocation_total",
                    "disk.capacity_total",
                    "disk.physical_total",
                    "host_alive_status",
                    "instance",
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
                    "vm.disk.allocation_total",
                    "vm.disk.capacity_total",
                    "vm.disk.physical_total",
                    "vm.host_alive_status",
                    "vm.io.errors_total",
                    "vm.io.errors_total_sec",
                    "vm.io.read_bytes_total",
                    "vm.io.read_bytes_total_sec",
                    "vm.io.read_ops_total",
                    "vm.io.read_ops_total_sec",
                    "vm.io.write_bytes_total",
                    "vm.io.write_bytes_total_sec",
                    "vm.io.write_ops_total",
                    "vm.io.write_ops_total_sec"
                    "vm.mem.free_mb",
                    "vm.mem.free_perc",
                    "vm.mem.resident_mb",
                    "vm.mem.swap_used_mb",
                    "vm.mem.total_mb",
                    "vm.mem.used_mb",
                    "vm.ping_status"]
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
                         "vm.io.write_ops_sec", ]
    vswitch_metric_names = ["vm.vswitch.in_bytes",
                            "vm.vswitch.in_bytes_sec",
                            # "vm.vswitch.in_bits",
                            # "vm.vswitch.in_bits_sec",
                            "vm.vswitch.in_packets",
                            "vm.vswitch.in_packets_sec",
                            "vm.vswitch.in_dropped",
                            "vm.vswitch.in_dropped_sec",
                            "vm.vswitch.in_errors",
                            "vm.vswitch.in_errors_sec",
                            "vm.vswitch.out_bytes",
                            "vm.vswitch.out_bytes_sec",
                            # "vm.vswitch.out_bits",
                            # "vm.vswitch.out_bits_sec",
                            "vm.vswitch.out_packets",
                            "vm.vswitch.out_packets_sec",
                            "vm.vswitch.out_dropped",
                            "vm.vswitch.out_dropped_sec",
                            "vm.vswitch.out_errors",
                            "vm.vswitch.out_errors_sec",
                            "vswitch.in_bytes",
                            "vswitch.in_bytes_sec",
                            # "vswitch.in_bits",
                            # "vswitch.in_bits_sec",
                            "vswitch.in_packets",
                            "vswitch.in_packets_sec",
                            "vswitch.in_dropped",
                            "vswitch.in_dropped_sec",
                            "vswitch.in_errors",
                            "vswitch.in_errors_sec",
                            "vswitch.out_bytes",
                            "vswitch.out_bytes_sec",
                            # "vswitch.out_bits",
                            # "vswitch.out_bits_sec",
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

    def __init__(self, resource_id, admin_tenant_id, tenant_id, region):
        self.resource_id = resource_id
        self.admin_tenant_id = admin_tenant_id or "tenant_1"
        self.vm_tenant_id = tenant_id or "vm_tenant_1"
        self.region = region or "region_1"

        self.metric_ids = set()
        self.base_dimensions = {
            "cloud_name": "test_cloud",
            "cluster": "test_cluster",
            "service": "compute",
            "resource_id": str(resource_id),
            "zone": "nova",
            "component": "vm",
            "hostname": "test_vm_host_" + str(resource_id)
        }

        self.preload_metrics()

    def preload_metrics(self):
        for name in vmSimulator.metric_names:
            dimensions = self.base_dimensions.copy()
            tenant_id = self.vm_tenant_id

            if name.startswith('vm.'):
                dimensions['tenant_id'] = self.vm_tenant_id
                tenant_id = self.admin_tenant_id

            self.metric_ids.add(add_full_definition(name=name,
                                                    dimensions=dimensions,
                                                    tenant_id=tenant_id,
                                                    region=REGION))

        for name in vmSimulator.disk_metric_names:
            for disk in vmSimulator.disks:
                dimensions = self.base_dimensions.copy()
                tenant_id = self.vm_tenant_id

                if name.startswith('vm.'):
                    dimensions['tenant_id'] = self.vm_tenant_id
                    tenant_id = self.admin_tenant_id

                dimensions['device'] = disk
                self.metric_ids.add(add_full_definition(name=name,
                                                        dimensions=dimensions,
                                                        tenant_id=tenant_id,
                                                        region=REGION))

        for name in vmSimulator.network_metric_names:
            for device in vmSimulator.network_devices:
                dimensions = self.base_dimensions.copy()
                tenant_id = self.vm_tenant_id

                if name.startswith('vm.'):
                    dimensions['tenant_id'] = self.vm_tenant_id
                    tenant_id = self.admin_tenant_id

                dimensions['device'] = device
                self.metric_ids.add(add_full_definition(name=name,
                                                        dimensions=dimensions,
                                                        tenant_id=tenant_id,
                                                        region=REGION))

        for name in vmSimulator.vswitch_metric_names:
            for switch in vmSimulator.vswitches:
                dimensions = self.base_dimensions.copy()
                tenant_id = self.vm_tenant_id

                if name.startswith('vm.'):
                    dimensions['tenant_id'] = self.vm_tenant_id
                    tenant_id = self.admin_tenant_id

                dimensions['device'] = switch
                self.metric_ids.add(add_full_definition(name=name,
                                                        dimensions=dimensions,
                                                        tenant_id=tenant_id,
                                                        region=REGION))

    def get_metric_ids(self):
        return self.metric_ids

    @staticmethod
    def get_total_metric_defs():
        base_defs = len(vmSimulator.metric_names)
        disk_defs = len(vmSimulator.disk_metric_names) * len(vmSimulator.disks)
        network_defs = len(vmSimulator.network_metric_names) * len(vmSimulator.network_devices)
        vswitch_defs = len(vmSimulator.vswitch_metric_names) * len(vmSimulator.vswitches)

        return base_defs + disk_defs + network_defs + vswitch_defs


def add_measurement(def_dim_id, timestamp):
    value = str(random.randint(0, 1000000))
    meas_list.append(','.join([def_dim_id, timestamp, value]) + '\n')


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


def flush_definition_data():
    global def_dims_list
    def_dims_temp = open(DEF_DIMS_FILENAME, 'w')
    def_dims_temp.write('\n'.join(def_dims_list) + '\n')
    def_dims_temp.close()
    def_dims_list = []

    global def_list
    def_temp = open(DEFINITIONS_FILENAME, 'w')
    def_temp.write('\n'.join(def_list) + '\n')
    def_temp.close()
    def_list = []

    global dims_list
    dims_temp = open(DIMENSIIONS_FILENAME, 'w')
    dims_temp.write('\n'.join(dims_list) + '\n')
    dims_temp.close()
    dims_list = []

    query = DEFINITION_COPY_QUERY.format(DEF_DIMS_FILENAME,
                                         DEFINITIONS_FILENAME,
                                         DIMENSIIONS_FILENAME)
    run_query(query)


def flush_measurement_data():
    global meas_list
    meas_temp = open(MEASUREMENTS_FILENAME, 'w')
    meas_temp.write('\n'.join(meas_list) + '\n')
    meas_temp.close()
    meas_list = []

    query = MEASUREMENT_COPY_QUERY.format(MEASUREMENTS_FILENAME)

    run_query(query)


def id_generator(size=32, chars=string.hexdigits):
    return ''.join(random.choice(chars) for _ in range(size))


def fill_metrics(number_of_days, new_vms_per_hour):
    vm_tenant_ids = [id_generator(ID_SIZE) for _ in range(TOTAL_VM_TENANTS)]
    metrics_per_vm = vmSimulator.get_total_metric_defs()
    expected_definitions = NUMBER_OF_DAYS * 24 * NEW_VMS_PER_HOUR * metrics_per_vm

    active_vms = []
    start_time = time.time()
    base_timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=NUMBER_OF_DAYS)
    for x in xrange(number_of_days):
        for y in xrange(24):
            for z in xrange(new_vms_per_hour):
                global next_resource_id
                active_vms.append(vmSimulator(resource_id=next_resource_id,
                                              admin_tenant_id=TENANT_ID,
                                              tenant_id=random.choice(vm_tenant_ids),
                                              region=REGION))
                next_resource_id += 1

            if len(active_vms) > TOTAL_ACTIVE_VMS:
                active_vms = active_vms[new_vms_per_hour:]

            for zz in xrange(measurements_per_hour):
                timestamp = base_timestamp + datetime.timedelta(days=x, hours=y, seconds=(30 * zz))
                formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                for vm in active_vms:
                    for metric_id in vm.get_metric_ids():
                        add_measurement(metric_id, formatted_timestamp)
                        if FULL_MEASUREMENTS and len(meas_list) > (LOCAL_STORAGE_MAX * 10):
                            print("Flushing Measurements")
                            num_measurements = len(meas_list)
                            flush_measurement_data()
                            time_delta = (time.time() - start_time)
                            print("{0:.0f} measurements / sec".format(num_measurements / time_delta))

            if len(def_dims_list) > LOCAL_STORAGE_MAX:
                print("Flushing Definitions")
                flush_definition_data()
                time_delta = (time.time() - start_time)
                print("{0:.0f} def / sec".format(len(def_dim_id_set) / time_delta))
                print("{0:.2f} %".format(len(def_dim_id_set) / float(expected_definitions) * 100))

    flush_definition_data()
    flush_measurement_data()


def run_query(query):
    command = ["/opt/vertica/bin/vsql",
               "-U" + CONN_INFO['user'],
               "-w" + CONN_INFO['password'],
               "-c", query]

    sub_output = subprocess.check_output(command)
    return '\n'.join(sub_output.splitlines()[2:-2])


def vertica_db_filler():
    if CLEAR_METRICS:
        print("Removing all vertica data")
        query = "TRUNCATE TABLE MonMetrics.DefinitionDimensions; " \
                "TRUNCATE TABLE MonMetrics.Definitions; " \
                "TRUNCATE TABLE MonMetrics.Dimensions; " \
                "TRUNCATE TABLE MonMetrics.Measurements;"
        run_query(query)

    # print("New definitions per day: {}".format(definitions_per_day))
    print("Creating metric history for the past {} days".format(NUMBER_OF_DAYS))
    cProfile.run('fill_metrics(NUMBER_OF_DAYS, NEW_VMS_PER_HOUR)')
    print("  Created {} definitions total".format(len(def_dim_id_set)))

    print("Checking if data arrived...")
    print(" DefinitionDimensions")
    print(run_query("SELECT count(*) FROM MonMetrics.DefinitionDimensions;"))
    print(" Definitions")
    print(run_query("SELECT count(*) FROM MonMetrics.Definitions;"))
    print(" Dimensions")
    print(run_query("SELECT count(*) FROM MonMetrics.Dimensions;"))
    print(" Measurements")
    print(run_query("SELECT count(*) FROM MonMetrics.Measurements;"))

    print('Finished loading VDB')


if __name__ == "__main__":
    sys.exit(vertica_db_filler())
