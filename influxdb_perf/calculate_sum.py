import re


def parse_data(path):
    try:
        with open(path, 'r') as f:
            results = []
            sum_result = 0
            for line in f:
                line = line.rstrip()
                if not line:
                    continue
                result = re.match("measurements per sec:.*?(\d+).*", line)
                if result:
                    results.append(float(result.group(1)))
                    sum_result += float(result.group(1))
    except Exception:
        import traceback
        print("error on file: {}".format(path))
        traceback.print_exc()
    print "results = {}".format(results)
    print "sum_result = {}".format(sum_result)

parse_data('/home/blank/influxdb_repo/test_scripts/data.txt')
