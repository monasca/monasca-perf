monasca-perf
============

a few performance testing tools

perf.py - sends x requests as fast as it can to the api and exits.
  
  time python perf.py

agent_simulator.py - sends requests once (default) or continously to the api.
  Simulates agent sends by having a configurable send interval per process(server).

  python agent_simulatory.py

perfprocess.py - sends x requests as fast as it can and exits.
  
  time python perfprocess.py

check.py - after restarting the persisters, run this on the persister server for stats.
  
  python check.py
