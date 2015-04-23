import datetime

from monascaclient import client
from monascaclient import ksclient

endpoint = "https://mon-ae1test-monasca01.useast.hpcloud.net:8080/v2.0"
auth_url = "http://10.22.156.20:35358/v3/"


def mon_client(username, password, auth_url, endpoint):
    kwargs = {
        'username': username,
        'password': password,
        'auth_url': auth_url
    }

    _ksclient = ksclient.KSClient(**kwargs)
    kwargs = {'token': _ksclient.token}
    api_version = '2_0'
    return client.Client(api_version, endpoint, **kwargs)


c = mon_client("mini-mon", "password", auth_url, endpoint)

m = c.metrics

start = datetime.datetime.now()

ago = datetime.timedelta(days=60)

start = start - ago

aggregate = {}

unique_metrics = {}
for metric in m.list():
    unique_metrics[metric['name']] = 1

for metric in sorted(unique_metrics.keys()):
    print metric
    statistics = m.list_statistics(name=metric,
                                   statistics="count",
                                   period="1000000000",
                                   merge_metrics=True,
                                   start_time=start.isoformat())
    for stat in statistics:
        key = stat['name']
        count = stat['statistics'][0][1]
        print "   {} -> {}".format(key, count)
        if key in aggregate:
            aggregate[key] += count
        else:
            aggregate[key] = count


print "total measurements = {}".format(sum(aggregate.values()))
