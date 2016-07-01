import datetime
import hashlib
import random
import subprocess
import sys

""" vertica_db_filler
    Run this after simulating the number of nodes, or in a performance testing configuration
    that has the desired number of metric definitions.
    This will use the existing dimensions in the DB to add measurements for each metric definition.
"""

# Clear the current metrics from the DB for testing
CLEAR_METRICS = True
# Total definitions active at one time
TOTAL_ACTIVE_DEFINITIONS = 800
# Number of new metric definitions per hour
NEW_VMS_PER_HOUR = 80
# will fill x number days backwards from current day including current day
NUMBER_OF_DAYS = 10

CONN_INFO = {'user': 'dbadmin',
             'password': 'password'
             }

# Each new definition set will have 'unique_key' with a unique value. Add other dimensions
# with dynamic values {day} {hour} {definition}
BASE_DIMENSIONS = {"day": "{day}",
                   "hour": "{hour}",
                   "definition": "{definition}"}

# Tenant id to report metrics under
TENANT_ID = "7e04ac703b024275aca4a9b3203847c8"
# Region in which to report metrics
REGION = "Region 1"



DEF_DIMS_FILENAME = '/tmp/defdims.dat'

DEFINITIONS_FILENAME = '/tmp/definitions.dat'

DIMENSIIONS_FILENAME = '/tmp/dimensions.dat'

MEASUREMENTS_FILENAME = '/tmp/measurements.dat'

COPY_QUERY = "COPY MonMetrics.DefinitionDimensions(id,definition_id,dimension_set_id) FROM '{}' " \
             "DELIMITER ',' COMMIT; " \
             "COPY MonMetrics.Definitions(id,name,tenant_id,region) FROM '{}' " \
             "DELIMITER ',' COMMIT; " \
             "COPY MonMetrics.Dimensions(dimension_set_id,name,value) FROM '{}' " \
             "DELIMITER ',' COMMIT; " \
             "COPY MonMetrics.Measurements(definition_dimensions_id,time_stamp,value) FROM '{}' " \
             "DELIMITER ',' COMMIT; "

def_id_set = set()
dim_id_set = set()
def_dim_id_set = set()
def_list = []
dims_list = []
def_dims_list = []
meas_list = []

next_resource_id = 1

ID_SIZE = 20


class vmSimulator(object):

    def __init__(self, resource_id, tenant_id, region):
        self.resource_id = resource_id
        self.tenant_id = tenant_id or "tenant_1"
        self.vm_tenant_id = "vm_tenant_1"
        self.region = region or "region_1"
        self.metrics = {}
        self.base_dimensions = {
            "cloud_name": "test_cloud",
            "cluster": "test_cluster",
            "service": "compute",
            "resource_id": str(resource_id),
            "zone": "nova",
            "component": "vm",
            "hostname": "test_vm_host_" + str(resource_id)
        }
        self.disks = ["sda", "sdb", "sdc", "sdd"]
        self.network_devices = ['tap1']
        self.metric_names = ["cpu.utilization_norm_perc",
                             "cpu.utilization_perc",
                             "disk.ephemeral.size",
                             "disk.root.size",
                             "host_alive_status",
                             "instance",
                             "io.errors_sec",
                             "io.read_bytes_sec",
                             "io.read_ops_sec",
                             "io.write_bytes_sec",
                             "io.write_ops_sec",
                             "memory",
                             "net.in_bytes",
                             "net.in_bytes_sec",
                             "net.in_packets",
                             "net.in_packets_sec",
                             "net.out_bytes",
                             "net.out_bytes_sec",
                             "net.out_packets",
                             "net.out_packets_sec",
                             "vcpus",
                             "vm.cpu.time_ns",
                             "vm.cpu.utilization_norm_perc",
                             "vm.cpu.utilization_perc",
                             "vm.host_alive_status",
                             "vm.io.errors",
                             "vm.io.errors_sec",
                             "vm.io.errors_total",
                             "vm.io.errors_total_sec",
                             "vm.io.read_bytes",
                             "vm.io.read_bytes_sec",
                             "vm.io.read_bytes_total",
                             "vm.io.read_bytes_total_sec",
                             "vm.io.read_ops",
                             "vm.io.read_ops_sec",
                             "vm.io.read_ops_total",
                             "vm.io.read_ops_total_sec",
                             "vm.io.write_bytes",
                             "vm.io.write_bytes_sec",
                             "vm.io.write_bytes_total",
                             "vm.io.write_bytes_total_sec",
                             "vm.io.write_ops",
                             "vm.io.write_ops_sec",
                             "vm.io.write_ops_total",
                             "vm.io.write_ops_total_sec",
                             "vm.net.in_bytes",
                             "vm.net.in_bytes_sec",
                             "vm.net.in_packets",
                             "vm.net.in_packets_sec",
                             "vm.net.out_bytes",
                             "vm.net.out_bytes_sec",
                             "vm.net.out_packets",
                             "vm.net.out_packets_sec"]

    def create_metrics(self):
        for name in self.metric_names:
            dimensions = self.base_dimensions.copy()
            if name.startswith('vm.'):
                dimensions['tenant_id'] = self.vm_tenant_id
            if 'disk' in name:
                for disk in self.disks:
                    dimensions['device'] = disk
                    yield (name, dimensions)
                continue

            if 'io' in name and 'total' not in name:
                for disk in self.disks:
                    dimensions['device'] = disk
                    yield (name, dimensions)
                continue

            if 'net' in name:
                for device in self.network_devices:
                    dimensions['device'] = device
                    yield (name, dimensions)
                continue

            yield (name, dimensions)


def add_measurement(def_dim_id, timestamp):
    formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    value = str(random.randint(0, 1000000))
    meas_list.append(','.join([def_dim_id, formatted_timestamp, value]) + '\n')


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
        add_dimension_set(dimensions, dimension_set_id)
        dim_id_set.add(dimension_set_id)

    if def_dim_id is None:
        id_hash = hashlib.sha1()
        id_hash.update(str(definition_id) + str(dimension_set_id))
        def_dim_id = id_hash.hexdigest()

    if def_dim_id not in def_dim_id_set:
        def_dims_list.append(','.join([def_dim_id, definition_id, dimension_set_id]))
        def_dim_id_set.add(def_dim_id)

    return def_dim_id


def add_dimension_set(dimensions, dimension_set_id):
    data = []
    for key in dimensions.iterkeys():
        data.append(','.join([dimension_set_id, key, dimensions[key]]))

    dims_list.append('\n'.join(data))


def set_dimension_values(active_dimensions, base_dimensions, day, hour, definition):
    for key in base_dimensions.keys():
        active_dimensions[key] = base_dimensions[key].format(day=day,
                                                             hour=hour,
                                                             definition=definition)


def flush_data():
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

    global meas_list
    meas_temp = open(MEASUREMENTS_FILENAME, 'w')
    meas_temp.write('\n'.join(meas_list) + '\n')
    meas_temp.close()
    meas_list = []

    print("Writing to vertica")
    query = COPY_QUERY.format(DEF_DIMS_FILENAME,
                              DEFINITIONS_FILENAME,
                              DIMENSIIONS_FILENAME,
                              MEASUREMENTS_FILENAME)
    run_query(query)


def fill_metrics(number_of_days, new_vms_per_hour):
    id_list = []
    active_vms = []
    for x in xrange(number_of_days):
        for y in xrange(24):
            for z in xrange(new_vms_per_hour):

                # active_dimensions = {}
                # set_dimension_values(active_dimensions, BASE_DIMENSIONS, day=str(x),
                #                      hour=str(y), definition=str(z))
                # active_dimensions["unique_key"] = str(x) + '-' + str(y) + '-' + str(z)
                # new_id = add_full_definition("api-test-0", active_dimensions,
                #                              tenant_id=TENANT_ID)
                global next_resource_id
                active_vms.append(vmSimulator(resource_id=next_resource_id, tenant_id=TENANT_ID,
                                              region=REGION))
                next_resource_id += 1

            if len(active_vms) > TOTAL_ACTIVE_DEFINITIONS:
                active_vms = active_vms[new_vms_per_hour:]

            for vm in active_vms:
                for metric in vm.create_metrics():
                    new_id = add_full_definition(metric[0], metric[1], vm.tenant_id, vm.region)
                    timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=x, hours=y)
                    add_measurement(new_id, timestamp)

            if len(def_dims_list) > 1000000:
                flush_data()

    flush_data()

    return id_list


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
    ids = fill_metrics(NUMBER_OF_DAYS, NEW_VMS_PER_HOUR)
    print("  Created {} definitions total".format(len(ids)))

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