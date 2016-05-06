# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
#
# Queries the Monasca Java Persister metrics endpoint to print out a
# summary of what the perister is doing. It does an initialization query,
# and then every minute does another query and prints out the what the
# persister has done in that minute
#

import httplib2
import json
import re
import time

METRIC_EVENTS = re.compile('monasca.persister.pipeline.event.MetricHandler\[metric-\d\].events-processed-meter')
METRIC_FLUSHES = re.compile('monasca.persister.pipeline.event.MetricHandler\[metric-\d\].flush-meter')

ALARM_EVENTS = re.compile('monasca.persister.pipeline.event.MetricHandler\[alarm-state-transition-\d\].events-processed-meter')
ALARM_FLUSHES = re.compile('monasca.persister.pipeline.event.MetricHandler\[alarm-state-transition-\d\].flush-meter')

METRIC_CACHE_HIT_METER = 'monasca.persister.repository.vertica.VerticaMetricRepo.definition-dimension-cache-hit-meter'
METRIC_CACHE_MISS_METER = 'monasca.persister.repository.vertica.VerticaMetricRepo.definition-dimension-cache-miss-meter'

DEFINITION_CACHE_HIT_METER = 'monasca.persister.repository.vertica.VerticaMetricRepo.definition-cache-hit-meter'
DEFINITION_CACHE_MISS_METER = 'monasca.persister.repository.vertica.VerticaMetricRepo.definition-cache-miss-meter'

DIMENSION_CACHE_HIT_METER = 'monasca.persister.repository.vertica.VerticaMetricRepo.dimension-cache-hit-meter'
DIMENSION_CACHE_MISS_METER = 'monasca.persister.repository.vertica.VerticaMetricRepo.dimension-cache-miss-meter'


def get_metrics():
    resp, content = httplib2.Http().request('http://localhost:8091/metrics')
    return json.loads(content)


def get_meters(meters, counter_re):
    measurement_counters = []
    for meter in meters:
        if counter_re.match(meter):
            measurement_counters.append(meters[meter])
    return measurement_counters


def get_count(meters, counter_re):
    measurement_counters = get_meters(meters, counter_re)
    count = 0
    for meter in measurement_counters:
        count += meter['count']
    return count


def get_counters():
    metrics = get_metrics()
    meters = metrics['meters']
    results = {}
    results['measurements'] = get_count(meters, METRIC_EVENTS)
    results['measurement_flushes'] = get_count(meters, METRIC_FLUSHES)
    results['measurement_threads'] = len(get_meters(meters, METRIC_FLUSHES))
    results['alarms'] = get_count(meters, ALARM_EVENTS)
    results['alarm_flushes'] = get_count(meters, ALARM_FLUSHES)
    results['alarm_threads'] = len(get_meters(meters, ALARM_FLUSHES))
    results['metric_hits'] = meters[METRIC_CACHE_HIT_METER]['count']
    results['metric_misses'] = meters[METRIC_CACHE_MISS_METER]['count']
    results['definition_hits'] = meters[DEFINITION_CACHE_HIT_METER]['count']
    results['definition_misses'] = meters[DEFINITION_CACHE_MISS_METER]['count']
    results['dimension_hits'] = meters[DIMENSION_CACHE_HIT_METER]['count']
    results['dimension_misses'] = meters[DIMENSION_CACHE_MISS_METER]['count']
    return results


def main():
    init_results = get_counters()

    while True:
        time.sleep(60)

        results = get_counters()

        print('{} Measurement Threads'.format(results['measurement_threads']))
        delta_measurements = results['measurements'] - init_results['measurements']
        print('{} measurements/min'.format(delta_measurements))

        delta_measurement_flushes = results['measurement_flushes'] - init_results['measurement_flushes']
        print('{} measurement flushes/min'.format(delta_measurement_flushes))

        delta_metric_hits = results['metric_hits'] - init_results['metric_hits']
        delta_metric_misses = results['metric_misses'] - init_results['metric_misses']
        delta_definition_hits = results['definition_hits'] - init_results['definition_hits']
        delta_definition_misses = results['definition_misses'] - init_results['definition_misses']
        delta_dimension_hits = results['dimension_hits'] - init_results['dimension_hits']
        delta_dimension_misses = results['dimension_misses'] - init_results['dimension_misses']

        print('{} total metrics, {} old {} new'.format(delta_metric_hits + delta_metric_misses, delta_metric_hits, delta_metric_misses))
        print('{} total definitions, {} old {} new'.format(delta_definition_hits + delta_definition_misses, delta_definition_hits, delta_definition_misses))
        print('{} total dimensions, {} old {} new'.format(delta_dimension_hits + delta_dimension_misses, delta_dimension_hits, delta_dimension_misses))

        print('{} Alarm Threads'.format(results['alarm_threads']))
        delta_alarms = results['alarms'] - init_results['alarms']
        print('{} alarms/min'.format(delta_alarms))

        delta_alarm_flushes = results['alarm_flushes'] - init_results['alarm_flushes']
        print('{} alarm flushes/min'.format(delta_alarm_flushes))
        print('\n')
        init_results = results


if __name__ == "__main__":
    main()
