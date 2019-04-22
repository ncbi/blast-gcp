"""Parse search log files and summarize results (requires pandas)"""

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
                start_time = pd.Timestamp(fields[4][1:].split('[', 1)[0])
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
                end_time = pd.Timestamp(fields[4][1:].split('[', 1)[0])
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
                        type=str,
                        help='Directory with logs (the report directory)',
                        default='report')
    parser.add_argument('--save-chunk-times', metavar='FILE',
                        dest='chunk_times', type=str,
                        help='Save search times against each chunk to a tab-delimited file')
    parser.add_argument('--save-chunk-hosts', metavar='FILE',
                        dest='chunk_hosts', type=str,
                        help='Save search locations for each chunk to a tab-delimited file')
    parser.add_argument('--hot-spots', dest='hotspots',
                        action='store_true', help='Analyze hotspots')
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

    prelim = pd.DataFrame()
    traceback = pd.DataFrame()

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
        prelim[rid] = df['PrelimTime']
        traceback[rid] = df['TracebackTime']

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
    start = df['Start'].sort_values().iloc[0]
    end = df['End'].sort_values().iloc[df.shape[0] - 1]
    wallclock_time = (end - start).seconds
    print('{} requests searched in {:.2f} min'.format(df.shape[0], wallclock_time / 60))
    print('{:.2f} requests per second'.format(df.shape[0] / wallclock_time))
    print('Time spent in BLAST searches: {:.2f} min'.format(
        chunk_time.sum().sum() / 1000 / 60))
    
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


    print('')
    print('Preliminary to traceback ratio:')
    print((prelim.sum() / traceback.sum()).quantile(
        q = [0.25, 0.5, 0.75, 0.9], interpolation = 'nearest'))

    # save hosts where database chunks where searched
    if args.chunk_hosts:
        chunks.to_csv(args.chunk_hosts, sep='\t', header=True)


    # save search times against each database chunks
    if args.chunk_times:
        chunk_time.to_csv(args.chunk_times, sep='\t', header=True)


    # generate a plot with run time per database chunk
    if args.hotspotplot:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

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
        
    
    # find database chunks that consitently take longer to search than others
    if args.hotspots:
        outliers = None
        
        # for each RID
        for rid in chunk_time:

            # find the position of a wisker in a box plot: Q3 + 1.5 * (Q3 - Q1),
            # everyting above this value is shown as an outlier in a box plot
            t = chunk_time[rid]
            q = t.quantile(q = [0.25, 0.5, 0.75], interpolation = 'nearest')
            wisker = q[0.75] + 1.5 * (q[0.75] - q[0.25])
            
            # subtract the wisker position from run times
            if outliers is None:
                outliers = pd.DataFrame({rid: t - wisker})
            else:
                outliers[rid] = t - wisker

        # find and count hotspots
        hotspots = (outliers > 0).sum(axis = 1).sort_values(ascending = False)
        print('')
        print('Hot spots:')
        print('Number of time a database chunk was a hotspot:')
        print(hotspots.head(20))
        
