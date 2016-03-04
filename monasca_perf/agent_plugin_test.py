import cProfile
import mock
import timeit

from monasca_agent.collector.checks_d.process import ProcessCheck as \
    agent_plugin

agent_config = {'autorestart': False, 'hostname': 'mini-mon',
                'dimensions': {'service': 'monitoring'},
                'listen_port': None, 'forwarder_url': 'http://localhost:17123',
                'additional_checksd': '/usr/lib/monasca/agent/custom_checks.d',
                'check_freq': 15, 'non_local_traffic': False, 'version': '1.1.23',
                'sub_collection_warn': 5, 'limit_memory_consumption': None,
                'skip_ssl_validation': False}

instance_config = [{'detailed': False, 'search_string': ['monasca-api'],
                    'dimensions': {'component': 'monasca-api', 'service': 'monitoring'},
                    'exact_match': False, 'built_by': 'MonAPI', 'name': 'monasca-api'},
                   {'detailed': False, 'search_string': ['monasca-notification'],
                    'dimensions': {'component': 'monasca-notification', 'service': 'monitoring'},
                    'exact_match': False, 'built_by': 'MonNotification',
                    'name': 'monasca-notification'}]


def test():
    mock_gauge = mock.MagicMock()
    p = agent_plugin("process", {}, agent_config, instance_config)
    p.gauge = mock_gauge

    p.prepare_run()

    for i in xrange(100):
        for instance in instance_config:
            p.check(instance)

if __name__ == '__main__':
    pr = cProfile.Profile()
    pr.enable()
    print(timeit.repeat("test()",
                        number=100,
                        repeat=3,
                        setup="from __main__ import test"))
    pr.create_stats()
    pr.print_stats(sort='tottime')
