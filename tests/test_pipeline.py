#!/usr/bin/env python3

import atexit
import json
import os
import getpass
import random
#import secrets # python 3.6
import subprocess
import sys
import threading
import time

# sudo apt update
# sudo apt install -y -u python python-dev python3 python3-dev
# easy_install --user pip
# pip install --upgrade virtualenv
# pip install --user --upgrade google-cloud-storage
# pip install --user --upgrade google-cloud-pubsub

# gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

from google.cloud import pubsub
from google.cloud import storage

# GLOBALS
CLUSTER_ID = ""
TEST_ID = ""
STORAGE_CLIENT = None
BUCKET = ""
BUCKET_NAME = ""
TESTS = []
PUBSUB_CLIENT = None
PUBSUB = ""
JOB_ID = ""


def get_cluster():
    global CLUSTER_ID
    user = getpass.getuser()
    print("\nuser is " + user)
    cmd = [
        'gcloud', 'dataproc', '--project', 'ncbi-sandbox-blast', '--region',
        'us-east4', 'clusters', 'list'
    ]
    results = subprocess.check_output(cmd)
    #    print(results)
    clusters = results.decode().split('\n')
    for cluster in clusters:
        cluster_name = cluster.split(' ')[0]
        if cluster_name.startswith("blast-dataproc-" + user):
            CLUSTER_ID = cluster_name
            return


#subscriber = pusub_v1.SubscriberClient()


def make_pubsub():
    #topic#_path = publisher.PUBSUB(
    global PUBSUB, PUBSUB_CLIENT, TEST_ID
    PUBSUB_CLIENT = pubsub.PublisherClient()
    PUBSUB = 'projects/{project_id}/topics/{topic}'.format(
        project_id="ncbi-sandbox-blast",  # os.getenv('GOOGLE_CLOUD_PROJECT'),
        topic=TEST_ID)

    # Create the topic.
    topic = PUBSUB_CLIENT.create_topic(PUBSUB)

    #topic=publisher.get_topic(PUBSUB)
    print('\nTopic created: {}'.format(topic))
    return PUBSUB


def make_bucket():
    global STORAGE_CLIENT, TEST_ID, BUCKET, BUCKET_NAME
    STORAGE_CLIENT = storage.Client()
    BUCKET_NAME = TEST_ID
    #    BUCKET = BUCKET.replace('_', '')
    print('\nCreating bucket ' + BUCKET_NAME)
    BUCKET = STORAGE_CLIENT.create_bucket(BUCKET_NAME)


def get_tests():
    global TESTS
    #    test_blobs.append(test)
    #for test in test_request_bucket.list_blobs():

    test_blobs = os.listdir('queries')
    random.shuffle(test_blobs)

    for test_file in test_blobs[0:5]:
        test_file = 'queries/' + test_file
        print('  ' + test_file)
        with open(test_file) as fin:
            read_data = fin.read()

        #foo=test.download_as_string()
        j = json.loads(read_data)
        j['orig_RID'] = j['RID']
        print("  RID was " + j['RID'])
        j['RID'] = TEST_ID + j['RID']
        print("  RID  is " + j['RID'])
        TESTS.append(j)
    print("Loaded " + str(len(TESTS)) + " tests")


def publish(jdict):
    global PUBSUB, PUBSUB_CLIENT
    PUBSUB_CLIENT.publish(PUBSUB, b'test')
    jdict['pubsub_submit_time'] = time.time()
    msg = json.dumps(jdict, indent=4).encode()
    #print(msg)
    PUBSUB_CLIENT.publish(PUBSUB, msg)


def make_json():
    global BUCKET_NAME, PUBSUB
    j = {}
    j['result_bucket'] = BUCKET_NAME
    j['subscript_id'] = PUBSUB
    j['log_request'] = 'true'
    j['log_partition_prep'] = "true"
    j['top_n'] = 100
    j['num_db_partitions'] = 886
    j['num_workers'] = 30
    j['num_executors'] = 8
    j['num_executor_cores'] = 5
    return json.dumps(j, indent=4)


def submit_application(config):
    global CLUSTER_ID, TEST_ID, JOB_ID
    jsonfname = TEST_ID + ".json"
    fout = open(jsonfname, "w")
    fout.write(config)
    fout.close()

    cmd = []
    cmd.append("gcloud")
    cmd.append("dataproc")
    cmd.append("jobs")
    cmd.append("submit")
    cmd.append("spark")
    cmd.append("--cluster")
    cmd.append(CLUSTER_ID)
    cmd.append("--class")
    cmd.append("gov.nih.nlm.ncbi.blastjni.BLAST_MAIN")
    cmd.append("--jars")
    cmd.append(
        "/home/vartanianmh/bigdata-interop/pubsub/target/spark-pubsub-0.1.0-SNAPSHOT-shaded.jar,../pipeline/target/sparkblast-1-jar-with-dependencies.jar"
    )
    cmd.append("--project")
    cmd.append("ncbi-sandbox-blast")
    cmd.append("--files")
    cmd.append("../pipeline/libblastjni.so," + jsonfname)
    cmd.append("--region")
    cmd.append("us-east4")
    cmd.append("--")
    cmd.append(jsonfname)
    print(cmd)
    print("  cmd: " + ' '.join(cmd))
    JOB_ID = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode()
    JOB_ID = JOB_ID.replace("\n", "\n        ")
    print("JOB_ID is    " + JOB_ID)
    os.remove(jsonfname)


def status_thread():
    while True:
        print("Status:")
        time.sleep(5)


def submit_thread():
    global TESTS
    print("Submit thread started:" + str(len(TESTS)))
    for test in TESTS:
        # Emulate 1..10 submissions a second
        time.sleep(random.randrange(0, 100) / 1000)
        publish(test)
        print("Submitted")
    print("Submit thread complete")


def results_thread():
    global STORAGE_CLIENT, BUCKET, BUCKET_NAME, TESTS
    while True:
        print("Results:")
        for result in BUCKET.list_blobs():
            print("  result:" + result)

        time.sleep(5)


def cleanup():
    global PUBSUB, PUBSUB_CLIENT, BUCKET, BUCKET_NAME, STORAGE_CLIENT
    if PUBSUB:
        print("Removing PUBSUB " + PUBSUB)
        PUBSUB_CLIENT = pubsub.PublisherClient()
        PUBSUB_CLIENT.delete_topic(PUBSUB)
    if BUCKET:
        for test in BUCKET.list_blobs():
            STORAGE_CLIENT.DeleteObject(test)
            print("Deleting " + test + " from bucket " + BUCKET_NAME)
        print("Deleting BUCKET " + BUCKET_NAME)
        #bucket=STORAGE_CLIENT.get_bucket(BUCKET_NAME)
        BUCKET.delete()
    # TODO: Kill application?


#        cmd = "gsutil -m rm -r gs://" + BUCKET
#        subprocess.check_output(cmd)


def main():
    global TEST_ID, CLUSTER_ID
    # register atexit
    atexit.register(cleanup)
    TEST_ID = "blast_test_" + hex(random.randint(0, sys.maxsize))[2:]
    print("TEST_ID is " + TEST_ID + '\n')

    get_tests()

    #TEST_ID="blast_test-" + secrets.token_urlsafe(6)
    # Instantiates a client

    # Look for cluster
    get_cluster()
    print("\nCluster id is:" + CLUSTER_ID)
    # Start if not found?
    # Create output bucket
    make_bucket()
    # Create pubsub queue
    make_pubsub()
    # configure json.ini
    config = make_json()
    #print(config)

    # submit application
    submit_application(config)
    # Start threads
    time.sleep(15)
    #  status
    threading.Thread(target=status_thread).start()
    #  submits
    threading.Thread(target=submit_thread).start()
    #  results
    threading.Thread(target=results_thread).start()


#   asn1 diff

if __name__ == "__main__":
    main()
