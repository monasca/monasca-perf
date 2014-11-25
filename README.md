monasca-perf
============

a few performance testing tools

perf.py - sends x requests as fast as it can to the api and exits.
  
  time python perf.py

agent_simulator.py - sends requests once (default) or continously to the api.
  Simulates agent sends by having a configurable send interval per process(server).

  python agent_simulatory.py

perfprocess.py - sends x requests as fast as it can and exits.
  
  time python perfprocess.py  num_processes  num_threads  num_requests_per_thread  num_metrics_per_request

check.py - after restarting the persisters, run this on the persister server for stats.
  
  python check.py

influx_load.py - does a bunch of metric writes to an influx cluster

  time python influx_load.py  num_processes  num_threads  num_requests_per_thread  num_metrics_per_request  series_name  db_username  db_password

influx_host_list.py - Lists the hosts with data stored on influxdb + handles agent amplification

  python influx_host_list.py  dbusername  dbpassword  url
