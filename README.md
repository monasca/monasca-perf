monasca-perf
============

a few performance testing tools

perfprocess.py - sends x requests as fast as it can and exits.
  
  to run it:
  
  time python perfprocess.py > out 2>&1 &
  tail -f out 
  
