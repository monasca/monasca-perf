import datetime
import logging

import jsonschema
from monascaclient import client
from monascaclient import ksclient

endpoint = "http://192.168.10.4:8080/v2.0"
auth_url = "http://192.168.10.5:35357/v3/"

logging.disable(logging.WARN)

type_table = {'unicode': "".decode("unicode-escape"),
              'str': "",
              'int': 0,
              'double': 0.0,
              'dict': {},
              'list': []}


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


def test_metric_list():
    metrics = mon_client("mini-mon",
                         "password",
                         auth_url,
                         endpoint).metrics

    result = metrics.list()

    assert len(result) > 0, "no metrics returned"

    schema = {'type': 'object',
              'properties':  {
                  'dimensions': {'type': 'object'},
                  'id': {'type': 'string'},
                  'name': {'type': 'string'}}}

    for metric in result:
        jsonschema.validate(metric, schema)


def test_metric_statistics():
    metrics = mon_client("mini-mon",
                         "password",
                         auth_url,
                         endpoint).metrics

    start = datetime.datetime.now()
    ago = datetime.timedelta(minutes=2)
    start = start - ago

    result = metrics.list_statistics(name="cpu.idle_perc",
                                     statistics="count",
                                     period="300",
                                     start_time=start.isoformat(),
                                     merge_metrics=True)

    schema = {'type': 'object',
              'properties': {
                  'dimensions': {'type': 'object'},
                  'statistics': {'type': 'array',
                                 'items': {'type': 'array',
                                           'items': [{'type': 'string'},
                                                     {'type': 'number',
                                                      'minimum': 0}]}},
                  'id': {'type': 'string'},
                  'columns': {'type': 'array',
                              'items': [{'type': 'string'},
                                        {'type': 'string'}]},
                  'name': {'type': 'string'}}}

    for statistic in result:
        jsonschema.validate(statistic, schema)


def test_measurement_list():
    metrics = mon_client("mini-mon",
                         "password",
                         auth_url,
                         endpoint).metrics

    start = datetime.datetime.now()
    ago = datetime.timedelta(minutes=2)
    start = start - ago

    result = metrics.list_measurements(name="cpu.idle_perc",
                                       start_time=start.isoformat(),
                                       merge_metrics=True)

    schema = {'type': 'object',
              'properties': {
                  'dimensions': {'type': 'object'},
                  'measurements': {'type': 'array',
                                   'items': {'type': 'array',
                                             'items': [{'type': 'string'},
                                                       {'type': 'number'},
                                                       {'type': 'object'}]}},
                  'columns': {'type': 'array',
                              'items': [{'type': 'string'},
                                        {'type': 'string'},
                                        {'type': 'string'}]},
                  'name': {'type': 'string'}}}

    for measure in result:
        jsonschema.validate(measure, schema)


def test_alarm_history_list():
    alarms = mon_client("mini-mon",
                        "password",
                        auth_url,
                        endpoint).alarms

    result = alarms.history_list()

    schema = {'type': 'object',
              'properties': {
                  'new_state': {'type': 'string'},
                  'timestamp': {'type': 'string'},
                  'metrics': {'type': 'array',
                              'items': [{'type': 'object',
                                         'properties': {
                                             'dimensions': {'type': 'object'},
                                             'id': {'type': 'null'},
                                             'name': {'type': 'string'}}}]},
                  'alarm_id': {'type': 'string'},
                  'reason': {'type': 'string'},
                  'reason_data': {'type': 'string'},
                  'sub_alarms': {'type': 'array'},
                  'old_state': {'type': 'string'},
                  'id': {'type': 'string'}}}

    for alarm in result:
        jsonschema.validate(alarm, schema)


def test_alarm_history():
    alarms = mon_client("mini-mon",
                        "password",
                        auth_url,
                        endpoint).alarms

    result = alarms.history_list()

    id = result[0]['alarm_id']

    result = alarms.history(alarm_id=id)

    schema = {'type': 'object',
              'properties': {
                  'new_state': {'type': 'string'},
                  'timestamp': {'type': 'string'},
                  'metrics': {'type': 'array',
                              'items': [{'type': 'object',
                                         'properties': {
                                             'dimensions': {'type': 'object'},
                                             'id': {'type': 'null'},
                                             'name': {'type': 'string'}}}]},
                  'alarm_id': {'type': 'string'},
                  'reason': {'type': 'string'},
                  'reason_data': {'type': 'string'},
                  'sub_alarms': {'type': 'array'},
                  'old_state': {'type': 'string'},
                  'id': {'type': 'string'}}}

    for alarm in result:
        jsonschema.validate(alarm, schema)


test_metric_list()
test_metric_statistics()
test_measurement_list()
test_alarm_history_list()
test_alarm_history()
