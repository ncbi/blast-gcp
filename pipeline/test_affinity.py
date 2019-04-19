"""Parse *.txt files in the report directory sorted by modificaion time and
report the number of searches against a chunk that were done on a different
worker node than for the previous search.

It must be run in the report directory.

Takes no parameters.

Prints to stdout, tab-delimited: search number, RID, number of volumes that
were searched on a different node than in the prevoius search, search time.
"""


import os
from collections import defaultdict
import sys

if __name__ == '__main__':

    files = [f for f in os.listdir('.') if f.endswith('.txt')]
    files.sort(key=os.path.getmtime)

    # we compare database chunk location to the last winodw_size jobs
    window_size = 1
    window = []
    count = 0
    # for each RID log file
    for fname in files:
        deltas = 0
        runtime = 0
        new_d = defaultdict(str)
        with open(fname) as f:
            # for each line in the log file
            for line in f:
                if 'search' not in line and 'traceback' not in line:
                    continue

                # extract data 
                fields = line.rstrip().split()
                host = fields[0][:-1]
                db = fields[1]
                chunk_time = int(fields[7])
                runtime += chunk_time
                new_d[db] = host


        if len(window) > 0:
            w_deltas = []
            # for each search in the last window_size searches
            for d in window:
                # compute numbed of databases that were searched at different
                # location
                w_deltas.append(len([k for k in new_d if new_d[k] != d[k]]))
            deltas = min(w_deltas)
        else:
            deltas = len(new_d)
            
        # update the list of the last window_size searches
        if len(window) > 0 and len(window) >= window_size:
            window.pop(0)
        window.append(new_d)
            

        # report results
        print('{}\t{}\t{}\t{}'.format(count, fname, deltas, runtime))
        count += 1

