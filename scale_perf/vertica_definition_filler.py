import datetime
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
TOTAL_ACTIVE_DEFINITIONS = 8000
# Number of new metric definitions per hour
DEFINITIONS_PER_HOUR = 800
# will fill x number days backwards from current day including current day
NUMBER_OF_DAYS = 2

# Each new definition set will have 'unique_key' with a unique value. Add other dimensions
# with dynamic values {day} {hour} {definition}
BASE_DIMENSIONS = {"day": "{day}",
                   "hour": "{hour}",
                   "definition": "{definition}"}

# Tenant id to report metrics under
TENANT_ID = "20e673b3b2064081bdd2b363aa024e8f"



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

CONN_INFO = {'user': 'dbadmin',
             'password': 'password'
             }

current_def_dims_id = 1
current_definition_id = 1
current_dimension_set_id = 1

def_dims_list = []
def_list = []
dims_list = []
meas_list = []

ID_SIZE = 20


def add_measurement(def_dim_id, timestamp):
    formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    meas_list.append(','.join([def_dim_id, formatted_timestamp, str(random.randint(0, 1000000))]) + '\n')


def add_full_definition(name, dimensions, tenant_id='tenant_1', region='region_1',
                        def_dim_id=None, definition_id=None, dimension_set_id=None):
    global current_def_dims_id
    if def_dim_id is None:
        def_dim_id = str(current_def_dims_id)
        current_def_dims_id += 1

    global current_definition_id
    if definition_id is None:
        definition_id = str(current_definition_id).rjust(ID_SIZE, '0')
        current_definition_id += 1

    global current_dimension_set_id
    if dimension_set_id is None:
        dimension_set_id = str(current_dimension_set_id).rjust(ID_SIZE, '0')
        current_dimension_set_id += 1

    def_list.append(','.join([definition_id, name, tenant_id, region]))
    add_dimension_set(dimensions, dimension_set_id)
    def_dims_list.append(','.join([def_dim_id, definition_id, dimension_set_id]))

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
    def_dims_temp.write('\n'.join(def_dims_list))
    def_dims_temp.close()
    def_dims_list = []

    global def_list
    def_temp = open(DEFINITIONS_FILENAME, 'w')
    def_temp.write('\n'.join(def_list))
    def_temp.close()
    def_list = []

    global dims_list
    dims_temp = open(DIMENSIIONS_FILENAME, 'w')
    dims_temp.write('\n'.join(dims_list))
    dims_temp.close()
    dims_list = []

    global meas_list
    meas_temp = open(MEASUREMENTS_FILENAME, 'w')
    meas_temp.write('\n'.join(meas_list))
    meas_temp.close()
    meas_list = []


def fill_metrics(number_of_days, definitions_per_hour):
    id_list = []
    active_ids = []
    for x in xrange(number_of_days):
        for y in xrange(24):
            for z in xrange(definitions_per_hour):
                active_dimensions = {}
                set_dimension_values(active_dimensions, BASE_DIMENSIONS, day=str(x),
                                     hour=str(y), definition=str(z))
                active_dimensions["unique_key"] = str(x) + '-' + str(y) + '-' + str(z)
                new_id = add_full_definition("api-test-0", active_dimensions,
                                             tenant_id=TENANT_ID)
                id_list.append(new_id)
                active_ids.append(new_id)

            if len(active_ids) > TOTAL_ACTIVE_DEFINITIONS:
                active_ids = active_ids[DEFINITIONS_PER_HOUR:]

            for m_id in active_ids:
                timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=x, hours=y)
                add_measurement(m_id, timestamp)

            if len(def_dims_list) > TOTAL_ACTIVE_DEFINITIONS:
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
    ids = fill_metrics(NUMBER_OF_DAYS, DEFINITIONS_PER_HOUR)
    print("  Created {} definitions total".format(len(ids)))
    print("Writing to vertica")
    query = COPY_QUERY.format(DEF_DIMS_FILENAME,
                              DEFINITIONS_FILENAME,
                              DIMENSIIONS_FILENAME,
                              MEASUREMENTS_FILENAME)
    run_query(query)

    print("Checking if data arrived...")
    print("  DefinitionDimensions")
    print(run_query("SELECT count(*) FROM MonMetrics.DefinitionDimensions;"))
    print("  Definitions")
    print(run_query("SELECT count(*) FROM MonMetrics.Definitions;"))
    print("  Dimensions")
    print(run_query("SELECT count(*) FROM MonMetrics.Dimensions;"))
    print("  Measurements")
    print(run_query("SELECT count(*) FROM MonMetrics.Measurements;"))

    print('Finished loading DB')

if __name__ == "__main__":
    return_code = 1
    try:
        vertica_db_filler()
        return_code = 0
    except Exception as e:
        print(e)
    finally:
        sys.exit(return_code)