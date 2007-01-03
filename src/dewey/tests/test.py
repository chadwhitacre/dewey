import os, sys, time
from datetime import datetime

start = time.time()
i = 0
for dirpath, files, dirs in os.walk('/'):
    i += 1
end = time.time()
total = end - start
sys.stdout.write('%d files seen in ' % i)
if total > 60:
    print '%d minute(s), %d seconds' % ((total/60), (total%60))
else:
    print '%d seconds' % total
