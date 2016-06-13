import datetime
import sys
import vertica_python

""" vertica_db_filler

    Run this after simulating the number of nodes, or in a performance testing configuration
    that has the desired number of metric definitions.
    This will use the existing dimensions in the DB to add measurements for each metric definition.

"""
SELECT_DIMENSIONS_QUERY = "SELECT TO_HEX(id) AS id,TO_HEX(dimension_set_id) AS dimension_set_id " \
                          "FROM MonMetrics.DefinitionDimensions WHERE TO_HEX(definition_id) = '{}';"
#INSERT_MEASUREMENT_QUERY = "INSERT /*+ direct */ INTO MonMetrics.Measurements " \
INSERT_MEASUREMENT_QUERY = "INSERT INTO MonMetrics.Measurements " \
                           "(definition_dimensions_id,time_stamp,value) " \
                           "VALUES (HEX_TO_BINARY('0x{}'),'{}',{});"

#conn_info = {'host': '127.0.0.1',
conn_info = {'host': '192.168.245.3',
             'port': 5433,
             'user': 'mon_persister',
             'password': 'm2asYs79Nmv',
             'database': 'mon',
             # 10 minutes timeout on queries
             'read_timeout': 600,
             # default throw error on invalid UTF-8 results
             'unicode_error': 'strict',
             # SSL is disabled by default
             'ssl': False}

# measurements per day to add to each metric definition (dimension set)
measurements_per_day = 1
# will fill x number days from current day including current day
number_of_days = 1


def create_measurements(current_timestamp, def_dim_id, connection):
    cur = connection.cursor()
    days = number_of_days
    while (days >= 0):
        new_timestamp = current_timestamp - datetime.timedelta(days=days)
        formatted_timestamp = new_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        for x in range(measurements_per_day):
            query = INSERT_MEASUREMENT_QUERY.format(def_dim_id, formatted_timestamp, x)
            cur.execute(query)
        days -= 1
    connection.commit()


def vertica_db_filler():

    connection = vertica_python.connect(**conn_info)

    cur = connection.cursor('dict')
    cur.execute("SELECT TO_HEX(id) as id,name from MonMetrics.Definitions")
    id_list = cur.fetchall()

    for id_dict in id_list:
        # find all definition dimensions and dimension sets
        print id_dict['name']
        query = SELECT_DIMENSIONS_QUERY.format(str(id_dict['id']))
        cur = connection.cursor('dict')
        cur.execute(query)
        dim_list = cur.fetchall()
        for dim in dim_list:
            create_measurements(datetime.datetime.utcnow(), dim['id'], connection)

    connection.close()
    print('Finished loading DB')


if __name__ == "__main__":
    sys.exit(vertica_db_filler())
