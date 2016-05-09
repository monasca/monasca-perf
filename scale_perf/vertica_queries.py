import sys
import argparse
from subprocess import Popen, PIPE


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vertica_password",
                        help="Vertica password for disk.sh and alarm_transitions.sh", default='password',
                        required=False)
    return parser.parse_args()


def split_result(results):
    result_dict = []
    results = results.split("\n")
    column_names = results.pop(0).split(",")
    for result in results:
        result_dict.append(zip(column_names, result.split(",")))
    return result_dict


def run_query(vertica_base_query, vertica_query):
    vertica_base_query.append(vertica_query)
    query = Popen(vertica_base_query, stdout=PIPE, stderr=PIPE)
    result, error_output = query.communicate()
    print vertica_query
    return result


def main():
    args = parse_args()
    # USE THIS QUERY IF PARSING RESULTS RETURNS THEM AS CSV
    # vertica_base_query =["/opt/vertica/bin/vsql", "-U", "dbadmin", "-w",  args.vertica_password, "-A", "-F", ",", "-c"]

    vertica_base_query = ["/opt/vertica/bin/vsql", "-U", "dbadmin", "-w", args.vertica_password, "-c"]
    print run_query(list(vertica_base_query), "SELECT node_name, projection_name, projection_schema, "
                                              "wos_used_bytes, ros_used_bytes, ros_count FROM "
                                              "projection_storage")
    print run_query(list(vertica_base_query), "SELECT * from resource_rejections")
    print run_query(list(vertica_base_query), "SELECT node_name, request_queue_depth, "
                                              "active_thread_count, open_file_handle_count, "
                                              "wos_used_bytes, ros_used_bytes, "
                                              "resource_request_reject_count, "
                                              "resource_request_timeout_count, "
                                              "disk_space_request_reject_count, "
                                              "failed_volume_reject_count FROM resource_usage")
    # WANT TO ADD TO MONITORING LATER
    # print run_query(list(vertica_base_query), "SELECT * FROM disk_resource_rejections;")
    # print run_query(list(vertica_base_query), "SELECT * FROM io_usage;")
    # print run_query(list(vertica_base_query), "SELECT * FROM cpu_usage;")
    # print run_query(list(vertica_base_query), "SELECT * FROM network_usage;")

    print run_query(list(vertica_base_query), "SELECT event_timestamp, node_name, request_id, transaction_id, "
                                              "statement_id, error_level, message, detail, hint from error_messages "
                                              "WHERE error_level in ('ERROR', 'WARNING', 'FATAL', 'PANIC')"
                                              "ORDER BY error_level")

    print run_query(list(vertica_base_query), "SELECT * FROM resource_queues;")

    print run_query(list(vertica_base_query), "SELECT operation_start_timestamp, node_name, operation_name, "
                                              "table_schema, table_name, projection_name, ros_count, "
                                              "total_ros_used_bytes FROM tuple_mover_operations "
                                              "WHERE is_executing = true AND operation_name = 'Moveout';")

    print run_query(list(vertica_base_query), "SELECT operation_start_timestamp, node_name, operation_name, "
                                              "table_schema, table_name, projection_name, ros_count, "
                                              "total_ros_used_bytes FROM tuple_mover_operations "
                                              "WHERE is_executing = true AND operation_name = 'Mergeout';")


if __name__ == "__main__":
    sys.exit(main())
