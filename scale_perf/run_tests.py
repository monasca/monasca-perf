import subprocess
import sys
import argparse
import time
import datetime
import os

# Available tests
# Disk.sh to check initial and final disk
# Disk_io.sh to check disk_io runs every thirty seconds
# Kafka_topics.sh checks the lags from kafka every thirty seconds
# Top.sh grabs the top output continuously throughout the tests
# Query_alarms
# Metrics per a second
# Alarm transistions
# VSQL Alarm transistions


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_api", help="Run script to emulate load on our API. Queries alarm-list from api",
                        action="store_true", required=False)
    parser.add_argument("--query_alarm_state", help="Query current alarms by state", action="store_true", required=False)
    parser.add_argument("--query_metrics_per_second", help="Query metrics per second", action="store_true",
                        required=False)
    parser.add_argument("--query_alarm_transitions", help="Query alarm transitions per a minute", action="store_true",
                        required=False)
    parser.add_argument("--output_directory",
                        help="Output directory to place result files. Defaults to current directory", default='',
                        required=False)
    parser.add_argument("--vertica_password",
                        help="Vertica password for disk.sh and alarm_transitions.sh", default='password',
                        required=False)
    parser.add_argument("--mysql_password", help="Password for monapi user for the query alarm states", required=False,
                        default='password')
    parser.add_argument("--monasca_api_url",
                        help="Monasca api url to use when querying. Example being http://192.168.10.4:8070/v2.0")
    parser.add_argument("--run_time",
                        help="How long, in mins, collection will run. Defaults to run indefinitely until the user hits"
                             " control c", required=False, type=int, default=None)

    return parser.parse_args()


def main():

    start_time = datetime.datetime.utcnow().isoformat()

    args = parse_args()
    test_processes = []

    with open(args.output_directory + 'initial_disk', "w") as stdout:
        subprocess.Popen("./disk.sh " + args.vertica_password, shell=True, stdout=stdout)

    kafka_process = subprocess.Popen("exec ./kafka_topics.sh " + args.output_directory + 'kafka_info', shell=True)
    top_process = subprocess.Popen("exec top -b -d 1 > " + args.output_directory + 'system_info', shell=True)

    if args.query_alarm_transitions:
        test_processes.append(subprocess.Popen("exec ./alarm_transitions.sh " + args.output_directory +
                                               'alarm_transitions ' + args.vertica_password, shell=True))

    if args.query_api:
        cmd_line = "python query_alarms.py --monasca_api_url " + args.monasca_api_url
        if args.run_time:
            cmd_line += " --run_time " + str(args.run_time)
        test_processes.append(subprocess.Popen(cmd_line, shell=True))
    if args.query_alarm_state:
        cmd_line = "python query_alarm_state.py --output_directory " + args.output_directory + " --mysql_password " + \
                   args.mysql_password
        if args.run_time:
            cmd_line += " --run_time " + str(args.run_time)
        test_processes.append(subprocess.Popen(cmd_line, shell=True))

    if args.run_time is None:
        try:
            kafka_process.wait()
        except KeyboardInterrupt:
            for p in test_processes:
                p.kill()
    else:
        time.sleep(args.run_time * 60)

    with open(args.output_directory + 'final_disk', "w") as stdout:
        subprocess.Popen("./disk.sh " + args.vertica_password, shell=True, stdout=stdout)

    kafka_process.kill()
    os.kill(top_process.pid, 9)

    if args.query_metrics_per_second:
        subprocess.call("python query_metrics_per_second.py " + start_time + " --output_directory " +
                        args.output_directory + " --monasca_api_url " + args.monasca_api_url, shell=True)


if __name__ == "__main__":
    sys.exit(main())

