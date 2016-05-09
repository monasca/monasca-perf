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
    return result


def parse_projection_stats(stats):
    print stats


def parse_resource_rejections(rejections):
    print rejections


def parse_resource_usage(usage):
    print usage


def main():
    args = parse_args()
    # vertica_base_query =["/opt/vertica/bin/vsql", "-U", "dbadmin", "-w",  args.vertica_password, "-A", "-F", ",", "-c"]
    vertica_base_query =["/opt/vertica/bin/vsql", "-U", "dbadmin", "-w",  args.vertica_password, "-c"]
    parse_projection_stats(run_query(list(vertica_base_query), "select node_name, projection_name, projection_schema, "
                                                               "wos_used_bytes, ros_used_bytes, ros_count from "
                                                               "projection_storage"))
    parse_resource_rejections(run_query(list(vertica_base_query), "select * from resource_rejections"))
    parse_resource_usage(run_query(list(vertica_base_query), "select node_name, request_queue_depth, "
                                                             "active_thread_count, open_file_handle_count, "
                                                             "wos_used_bytes, ros_used_bytes, "
                                                             "resource_request_reject_count, "
                                                             "resource_request_timeout_count, "
                                                             "disk_space_request_reject_count, "
                                                             "failed_volume_reject_count from resource_usage"))

if __name__ == "__main__":
    sys.exit(main())
