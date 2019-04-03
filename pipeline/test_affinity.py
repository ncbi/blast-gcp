"""Parse *.txt files in the report directory sorted by modificaion time and
report the number of searches against a chunk that were done on a different
worker node than for the previous search.

It must be run in the report directory.

Takes no parameters.

Prints to stdout, tab-delimited: search number, RID, number of volumes that
were searched on a different node than in the prevoius search.
"""


import os
from collections import defaultdict
import sys

if __name__ == '__main__':

    files = [f for f in os.listdir('.') if f.endswith('.txt')]
    files.sort(key=os.path.getmtime)

    d = defaultdict(str)
    count = 0
    for fname in files:
        deltas = 0
        with open(fname) as f:
            for line in f:
                if 'search' not in line:
                    continue

                fields = line.rstrip().split()
                host = fields[0][:-1]
                db = fields[1]
                
                if d[db] != host:
                    deltas += 1
                    d[db] = host

        print('{}\t{}\t{}'.format(count, fname, deltas))
        count += 1

