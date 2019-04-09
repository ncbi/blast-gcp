"""Parse search log files and summarize results"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from itertools import islice
import argparse
import os

def parse(filename):
    """Parse the txt file for a single BLAST RID and report full and chunk
    trun times, worker nodes where search was done, and time stamps.

    Arguments:
    filename - path to the txt file

    Returns:
    a dataframe indexed by database chunk name, run time for the reques,
    start time, and end time
    """
    prelim_hosts = {}
    prelim_times = {}
    prelim_num_results = {}

    traceback_times = {}
    traceback_num_results = {}

    with open(filename) as f:
        for line in f:
            fields = line.rstrip().split()

            if line.startswith('starting request'):
                rid = fields[2][1:-1]
                start_time = pd.Timestamp(fields[4][1:-7])
            elif 'search' in line:
                chunk = fields[1]
                prelim_hosts[chunk] = fields[0]
                prelim_times[chunk] = int(fields[7])
                prelim_num_results[chunk] = int(fields[4])
            elif 'traceback' in line:
                chunk = fields[1]
                traceback_times[chunk] = int(fields[7])
                traceback_num_results[chunk] = int(fields[4])
            elif 'done' in line:
                end_time = pd.Timestamp(fields[4][1:-7])
                runtime = int(fields[6])
                errors = int(fields[11])

        phosts = pd.Series(prelim_hosts)
        ptimes = pd.Series(prelim_times)
        presults = pd.Series(prelim_num_results)
        ttimes = pd.Series(traceback_times, dtype=np.int64)
        tresults = pd.Series(traceback_times, dtype=np.int64)

        df = pd.DataFrame({'Host': phosts, 'PrelimTime': ptimes,
                           'PrelimResults': presults})

        df['TracebackTime'] = ttimes
        df['TracebackResults'] = tresults

        df.fillna(0, inplace = True)
        df['TracebackTime'] = df['TracebackTime'].astype('int64')
        df['TracebackResults'] = df['TracebackResults'].astype('int64')

        return (df, runtime, start_time, end_time)

                
                
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Summarize BLAST on SPARK logs')
    parser.add_argument('--logs', metavar='DIR', dest='dir',
                        type=str, help='Directory with logs',
                        default='report')
    parser.add_argument('--hot-spot-plot', dest='hotspotplot',
                        action='store_true',
                        help='Generate database chunk search time for analysis of hot spots')


    args = parser.parse_args()

    start = {}
    end = {}
    rtime = {}
    max_chunk_time = {}
    max_chunk = {}
    p90 = {}

    chunks = None
    chunk_time = None

    # for each file matching *.txt
    files = [f for f in os.listdir(args.dir) if f.endswith('.txt')]
    for filename in files:
        # get RID from file name
        rid = filename[:-4]
        if rid.startswith('REQ_'):
            rid = rid[4:]

        # collect information from the log
        df, runtime, start_time, end_time = parse(args.dir + '/' + filename)

        # index values by RID
        start[rid] = start_time
        end[rid] = end_time
        rtime[rid] = runtime
        m = df['PrelimTime'] + df['TracebackTime']
        max_chunk_time[rid] = m.max()
        max_chunk[rid] = m.idxmax()

        p90[rid] = m.quantile(q = 0.9, interpolation = 'nearest')

        # get search times against database chunks
        if chunk_time is None:
            chunk_time = pd.DataFrame({rid: df['PrelimTime'] +
                                       df['TracebackTime']})
        else:
            chunk_time[rid] = df['PrelimTime'] + df['TracebackTime']

        # get hosts where chubk searches were performed
        if chunks is None:
            chunks = pd.DataFrame({rid: df['Host']})
        else:
            chunks[rid] = df['Host']
        

    # summarize runs
    df = pd.DataFrame({'Start': start, 'End': end, 'Time': rtime,
                       'MaxChunkTime': max_chunk_time,
                       'WorstChunk': max_chunk,
                       'Perc90th': p90})

    # Reports
    print('{} requests searched in {:.2f} min'.format(df.shape[0],
                                               df['Time'].sum() / 1000 / 60))
    print('{:.2f} requests per second'.format(df.shape[0] / (df['Time'].sum() / 1000)))
    print('Time spent in BLAST searches: {:.2f} min'.format(
        chunk_time.sum().sum() / 100 / 60))
    
    print('Latency percentiles [ms]:')
    print(df['Time'].quantile(q = [0.5, 0.75, 0.9, 0.95, 0.99],
                              interpolation = 'nearest'))
    
    chunks = chunks.transpose()
    affinity = {}
    for name, col in chunks.iteritems():
        affinity[name] = col.value_counts().max()

    affinity = pd.Series(affinity)
    print('')
    print('Affinity:')
    print ('Percentiles for the max number of times a chunk was searched on the same host:')
    print(affinity.quantile(q = [0.1, 0.25, 0.5, 0.75, 0.9],
                            interpolation = 'nearest'))

    # generate a plot with run time per database chunk
    if args.hotspotplot:
        # find RIDs with the longes chunk search times
        rids = [i for i, v in islice(chunk_time.max().sort_values(
                                         ascending = False).iteritems(), 5)]

    
        # and plot these times against chunk names
        labels = []
        for i in rids:
            plt.scatter(x = range(0, chunk_time.shape[0]), y = chunk_time[i])
            labels.append(i)
        plt.legend(labels)
        plt.savefig('plot.pdf')
        
    
