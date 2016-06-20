import hashlib
import json

from monascaclient import client
from monascaclient import ksclient

endpoint = "http://127.0.0.1:8070/v2.0"
auth_url = "http://127.0.0.1:35357/v3/"


def alarm_client():
    kwargs = {
        'username': "foo",
        'password': "bar",
        'project_name': "baz",
        'auth_url': auth_url
    }

    _ksclient = ksclient.KSClient(**kwargs)
    kwargs = {'token': _ksclient.token}
    api_version = '2_0'
    return client.Client(api_version, endpoint, **kwargs).alarms


def test_alarm_history():
    alarms = alarm_client()

    alarm = alarms.list()[0]

    print(alarm['id'])

    original_history = alarms.history(alarm_id=alarm['id'])

    hash = hashlib.md5(json.dumps(original_history)).hexdigest()

    print("base hash: {}".format(hash))

    for x in xrange(1000000):
        if x % 100 == 0:
            print("test: {}".format(x))
        if x % 1000 == 0:
            alarms = alarm_client()

        query_result = alarms.history(alarm_id=alarm['id'])

        query_hash = hashlib.md5(json.dumps(query_result)).hexdigest()

        if query_hash != hash:
            print("hash mismatch")
            print("-----------------------------")
            print(original_history)
            print("-----------------------------")
            print(query_result)
            print("-----------------------------")
            return

    print("All queries successful")

test_alarm_history()
