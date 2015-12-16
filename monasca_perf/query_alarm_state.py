import multiprocessing
import sys
import time
import argparse
import MySQLdb


query = ("select alarm.alarm_definition_id as definition_id, alarm_definition.name as definition_name, "
         "count(distinct alarm.id) as num_alarms from alarm join alarm_definition on alarm_definition.id = "
         "alarm.alarm_definition_id where alarm.state = '{0}' group by alarm.alarm_definition_id;")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait_time", help="Number of time between mysql queries (in seconds)", type=int,
                        required=False, default=120)
    parser.add_argument("--mysql_password", help="Password for monapi user", required=False, default='password')
    parser.add_argument("--mysql_host", help="Host running mysql we will connect to", required=False,
                        default='localhost')
    parser.add_argument("--output_directory",
                        help="Output directory to place result files. Defaults to current directory", required=False)
    parser.add_argument("--run_time",
                        help="How long, in mins, collection will run. Defaults to run indefinitely until the user hits"
                             " control c", required=False, type=int, default=None)
    return parser.parse_args()


def query_alarm_state(args, state):
    try:
        conn = MySQLdb.connect(
            host=args.mysql_host,
            user='monapi',
            passwd=args.mysql_password,
            db='mon')
    except MySQLdb.OperationalError, e:
        print(' MySQL connection failed: {0}'.format(e))
        return
    output_file_name = state.lower() + "_alarm_states"
    if args.output_directory:
        output_file_name = args.output_directory + output_file_name
    output_file = open(output_file_name, 'w')
    try:
        while True:
            conn.query(query.format(state))
            r = conn.store_result()
            data = r.fetch_row(maxrows=0)
            output_file.write(time.strftime("%c") + '\n')
            if not data:
                output_file.write("No current alarms for the state " + state + "\n")
            else:
                output_file.write('{:>50} {:>5}\n'.format("Alarm Definition", "Number of Alarms"))
            for row in data:
                output_file.write('{:>50} {:>5}\n'.format(row[1], row[2]))
            output_file.flush()
            time.sleep(args.wait_time)
    except KeyboardInterrupt:
        output_file.close()
        return


def query_alarms_test():

    args = parse_args()

    process_list = []

    p_undetermined = multiprocessing.Process(target=query_alarm_state, args=(args, 'UNDETERMINED'))
    p_ok = multiprocessing.Process(target=query_alarm_state, args=(args, 'OK'))
    p_alarm = multiprocessing.Process(target=query_alarm_state, args=(args, 'ALARM'))

    process_list.append(p_undetermined)
    process_list.append(p_ok)
    process_list.append(p_alarm)

    for p in process_list:
        p.start()

    if args.run_time is not None:
        time.sleep(args.run_time * 60)
        for p in process_list:
            p.terminate()
    else:
        try:
            for p in process_list:
                try:
                    p.join()
                except Exception:
                    pass
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    sys.exit(query_alarms_test())
