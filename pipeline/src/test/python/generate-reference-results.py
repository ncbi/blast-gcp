#!/usr/bin/env python3.6
"""
src/test/python/generate-reference-results.py - See DESC constant below

Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
Created: Fri 08 Mar 2019 04:13:02 PM EST
"""
import argparse
import logging
import subprocess
import shlex
import tempfile
import shutil
import os
import multiprocessing
from contextlib import contextmanager

VERSION = '0.1'
DFLT_LOGFILE = 'generate-reference-results.log'
DFLT_OUTDIR = os.getcwd()
DFLT_BLASTDB = '/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/orig_dbs'
DESC = r"""
Script to generate reference BLAST results for simple spark accuracy evaluation.

Overview:
   1. get csv file, split into fields
   2. build array of command lines, e.g.:
       blastn -db $DB -query $QFILE -outfmt 11 -out $OUTDIR/$RID.asn -num_threads $NT_ARG
   3. Run searches sequentially (since BLAST is MT)
   4. Abort if there are errors
"""
INPUT="gs://blast-test-requests-sprint11/webapp-input-single-queries.csv"


@contextmanager
def temp_query_dir():
    retval = tempfile.mkdtemp(prefix="blast-rids-")
    try:
        yield retval
    finally:
        try:
            shutil.rmtree(retval)
        except IOError:
            print("Cannot clean up temp query dir {}".format(retval))


def main():
    """ Entry point into this program. """
    parser = create_arg_parser()
    args = parser.parse_args()
    config_logging(args)

    cmd = "/usr/bin/gsutil cat {}".format(INPUT)
    try:
        result = subprocess.Popen(shlex.split(cmd), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, universal_newlines=True, encoding='utf-8')
        with temp_query_dir() as query_dir:
            jobs = []
            for line in result.stdout:
                line = line.rstrip()
                if line.startswith("#"):
                    continue
                fields = line.split(",")
                rid = fields[3]
                output_file = os.path.join(args.outdir, rid) + ".asn"
                try:
                    if os.path.getsize(output_file) > 100:
                        continue
                except OSError as e:
                    pass
                qfile = os.path.join(query_dir, "{}.fsa".format(rid))
                with open(qfile, 'w') as query_file:
                    query_file.write(fields[2])
                blast_cmd = "/usr/bin/{} -db {} -query {} -num_threads {} -outfmt 11 -out {}" \
                    .format(fields[0], os.path.join(args.blastdb, fields[1]), qfile, args.nt, output_file)
                jobs.append(blast_cmd)

            logging.debug("About to run {} BLAST searches".format(len(jobs)))
            for cmd in sorted(jobs, key=lambda line: line.split(" ")[2]):
                if args.dry_run:
                    print(cmd)
                else:
                    logging.debug(cmd)
                    subprocess.run(shlex.split(cmd), encoding='utf-8', universal_newlines=True,
                                   stdin=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
    except subprocess.SubprocessError as excp:
        logging.error("Failed to run {}: {}".format(cmd, excp.stderr))
        return 1

    return 0


def create_arg_parser():
    """ Create the command line options parser object for this script. """
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument("-outdir", default=DFLT_OUTDIR,
                        help="Directory to save output, default: " + DFLT_OUTDIR)
    parser.add_argument("-blastdb", default=DFLT_BLASTDB,
                        help="Directory where BLASTDBs reside: " + DFLT_BLASTDB)
    parser.add_argument("-num-threads", dest="nt", type=int, default=int(multiprocessing.cpu_count()/2),
                        help="Number of threads to use when running BLAST")
    parser.add_argument("-logfile", default=DFLT_LOGFILE,
                        help="Default: " + DFLT_LOGFILE)
    parser.add_argument("-dry_run", action='store_true', help="Print commands only")
    parser.add_argument("-loglevel", default='INFO',
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument('-V', '--version', action='version',
                        version='%(prog)s ' + VERSION)
    return parser


def config_logging(args):
    if args.logfile == 'stderr':
        logging.basicConfig(level=str2ll(args.loglevel),
                            format="%(asctime)s %(message)s")
    else:
        logging.basicConfig(filename=args.logfile, level=str2ll(args.loglevel),
                            format="%(asctime)s %(message)s", filemode='w')
    logging.logThreads = 0
    logging.logProcesses = 0
    logging._srcfile = None


def str2ll(level):
    """ Converts the log level argument to a numeric value.

    Throws an exception if conversion can't be done.
    Copied from the logging howto documentation
    """
    retval = getattr(logging, level.upper(), None)
    if not isinstance(retval, int):
        raise ValueError('Invalid log level: %s' % level)
    return retval


if __name__ == "__main__":
    import sys
    sys.exit(main())

