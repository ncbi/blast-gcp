#!/usr/bin/env python3
"""
archive-results.py - See DESC constant below

Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
Created: Tue 09 Apr 2019 03:11:02 PM EDT
"""
import argparse
import shutil
import tarfile
import os
import json
from datetime import datetime
from string import Template

VERSION = '0.1'
DFLT_REPORT_DIR = 'report'
DFLT_BUCKET = 'gs://blast-performance-test-results'
DESC = r"""\
Script to preserve the results of simple spark application test runs
"""


def main():
    """ Entry point into this program. """
    parser = create_arg_parser()
    args = parser.parse_args()
    data = {}
    with args.cfg as f:
        json_data = json.load(f)
        data['db'] = json_data['databases'][0]['key']
        data['parallel_jobs'] = json_data["cluster"]["parallel_jobs"]
        data['num_executor_cores'] = json_data["cluster"]["num_executor_cores"]
        data['num_executors'] = json_data["cluster"]["num_executors"]
        data['locality_wait'] = json_data["cluster"]["locality_wait"]
        data['num_partitions'] = json_data["cluster"]["num_partitions"]

    if args.verbose > 2:
        print("JSON data parsed")
        print(json.dumps(data, sort_keys=True, indent=4))
    tmpl = Template("spark-blast-results-$db-$num_partitions-$locality_wait-$parallel_jobs-$num_executors-$num_executor_cores")
    archive_name = tmpl.substitute(data)
    shutil.rmtree(archive_name, ignore_errors=True)

    if os.path.exists(args.report_dir) and os.path.isdir(args.report_dir):
        shutil.copytree(args.report_dir, archive_name)
    else:
        print("ERROR: No results directory '{}' found".format(args.report_dir))
        return 1
    shutil.copy(args.cfg.name, archive_name)

    archive = tarfile.open("{}.tgz".format(archive_name), "w:gz")
    archive.add(archive_name)
    archive.close()
    if args.verbose:
        print("Created archive {}.tgz".format(archive_name))

    shutil.rmtree(archive_name)
    ts = datetime.now()
    cmd = "gsutil -qm cp {}.tgz {}/{}/".format(archive_name, args.bucket, ts.strftime("%Y%m%d%H%M"))
    print(cmd)
    os.system(cmd)
    return 0


def create_arg_parser():
    """ Create the command line options parser object for this script. """
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument("cfg", type=argparse.FileType('r'), help="Configuration file to parse")
    parser.add_argument("-report_dir", default=DFLT_REPORT_DIR,
                        help="Directory containing simple spark application results, default: " + DFLT_REPORT_DIR)
    parser.add_argument("-bucket", default=DFLT_BUCKET,
                        help="Bucket to store the tarball with results, default: " + DFLT_BUCKET)
    parser.add_argument('-V', '--version', action='version',
                        version='%(prog)s ' + VERSION)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Increase output verbosity")
    return parser


if __name__ == "__main__":
    import sys
    sys.exit(main())

