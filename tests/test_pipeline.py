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
PROJECT = "ncbi-sandbox-blast"
CLUSTER_ID = ""
TEST_ID = ""
STORAGE_CLIENT = None
BUCKET = ""
BUCKET_NAME = ""
TESTS = []
PUBSUB_CLIENT = None
TOPIC = ""
SUBSCRIPTION = None
SUBSCRIPTION_PATH = ""
JOB_ID = ""
CONFIG_JSON = ""


def get_cluster():
    global CLUSTER_ID, PROJECT
    user = getpass.getuser()
    print("\nuser is " + user)
    cmd = [
        'gcloud', 'dataproc', '--project', PROJECT, '--region', 'us-east4',
        'clusters', 'list'
    ]
    results = subprocess.check_output(cmd)
    #    print(results)
    clusters = results.decode().split('\n')
    for cluster in clusters:
        cluster_name = cluster.split(' ')[0]
        if cluster_name.startswith("blast-dataproc-" + user):
            CLUSTER_ID = cluster_name
            return


def make_pubsub():
    #topic#_path = publisher.PUBSUB(
    global TOPIC, SUBSCRIPTION, SUBSCRIPTION_PATH, PUBSUB_CLIENT, TEST_ID
    global PROJECT
    PUBSUB_CLIENT = pubsub.PublisherClient()
    TOPIC = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT,  # os.getenv('GOOGLE_CLOUD_PROJECT'),
        topic=TEST_ID)

    # Create the topic.
    topic = PUBSUB_CLIENT.create_topic(TOPIC)

    #topic=publisher.get_topic(PUBSUB)
    print('\nTopic created: {}'.format(topic))

    subscriber = pubsub.SubscriberClient()

    SUBSCRIPTION_PATH = subscriber.subscription_path(PROJECT, TEST_ID)
    print("SUBSCRIPTION_PATH is " + SUBSCRIPTION_PATH)

    SUBSCRIPTION = subscriber.create_subscription(SUBSCRIPTION_PATH, TOPIC)

    print('Subscription created: {}'.format(SUBSCRIPTION))

    return


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
        # TODO: Randomly put query in gs bucket instead
        TESTS.append(j)
    print("Loaded " + str(len(TESTS)) + " tests")


def publish(jdict):
    global TOPIC, PUBSUB_CLIENT
    #    PUBSUB_CLIENT.publish(TOPIC, b'test')
    jdict['pubsub_submit_time'] = time.time()
    msg = json.dumps(jdict, indent=4).encode()
    #print(msg)
    PUBSUB_CLIENT.publish(TOPIC, msg)


def make_json():
    global BUCKET_NAME, SUBSCRIPTION_PATH, TEST_ID, PROJECT
    j = {}
    j['log_start'] = 'false'
    j['log_done'] = 'false'
    j['result_bucket'] = BUCKET_NAME
    j['subscript_id'] = SUBSCRIPTION_PATH
    j['log_request'] = 'true'
    j['log_worker_shift'] = 'false'
    j['batch_duration'] = 2
    j['trigger_host'] = ''
    j['trigger_port'] = 0
    j['log_partition_prep'] = "true"
    j['top_n'] = 100
    j['num_db_partitions'] = 886
    j['num_workers'] = 8
    j['num_executors'] = 8
    j['num_executor_cores'] = 3
    j['project_id'] = PROJECT
    return json.dumps(j, indent=4, sort_keys=True)


def submit_application(config):
    global CLUSTER_ID, TEST_ID, JOB_ID, CONFIG_JSON, PROJECT
    CONFIG_JSON = TEST_ID + ".json"
    fout = open(CONFIG_JSON, "w")
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
        "/home/vartanianmh/bigdata-interop/pubsub/target/spark-pubsub-0.1.0-SNAPSHOT-shaded.jar"
        + "," + "../pipeline/target/sparkblast-1-jar-with-dependencies.jar")
    cmd.append("--project")
    cmd.append(PROJECT)
    cmd.append("--files")
    cmd.append("../pipeline/libblastjni.so," + CONFIG_JSON)
    cmd.append("--region")
    cmd.append("us-east4")
    cmd.append("--")
    cmd.append(CONFIG_JSON)
    #print(cmd)
    print("  cmd: " + ' '.join(cmd))
    JOB_ID = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode()
    JOB_ID = JOB_ID.replace("\n", "\n        ")
    print("JOB_ID is    " + JOB_ID)


def status_thread():
    while True:
        print("Status...")
        time.sleep(5)


def submit_thread():
    global TESTS
    print("Submit thread started: " + str(len(TESTS)))
    for test in TESTS:
        # Emulate 1..10 submissions a second
        time.sleep(random.randrange(0, 100) / 1000)
        publish(test)
        print("Submitted")
    print("Submit thread complete")


def results_thread():
    global STORAGE_CLIENT, BUCKET, BUCKET_NAME, TESTS
    while True:
        print("Results...")
        for result in BUCKET.list_blobs():
            print("  result: " + result)


# TODO: Check Job Orchestration status
        time.sleep(5)


def cleanup():
    global PUBSUB_CLIENT, TOPIC, BUCKET, BUCKET_NAME
    global STORAGE_CLIENT, CONFIG_JSON, SUBSCRIPTION, SUBSCRIPTION_PATH
    if CONFIG_JSON:
        os.remove(CONFIG_JSON)
    if TOPIC:
        print("Removing TOPIC: " + TOPIC)
        PUBSUB_CLIENT = pubsub.PublisherClient()
        PUBSUB_CLIENT.delete_topic(TOPIC)
    if SUBSCRIPTION:
        print("Removing SUBSCRIPTION: " + SUBSCRIPTION_PATH)
        subscriber_client = pubsub.SubscriberClient()
        subscriber_client.delete_subscription(SUBSCRIPTION_PATH)
    if BUCKET:
        for test in BUCKET.list_blobs():
            STORAGE_CLIENT.DeleteObject(test)
            print("Deleting " + test + " from bucket " + BUCKET_NAME)
        print("Removing BUCKET: " + BUCKET_NAME)
        #bucket=STORAGE_CLIENT.get_bucket(BUCKET_NAME)
        BUCKET.delete()
    # TODO: Kill application?


#        cmd = "gsutil -m rm -r gs://" + BUCKET
#        subprocess.check_output(cmd)


def main():
    global TEST_ID, CLUSTER_ID
    # register atexit
    atexit.register(cleanup)
    TEST_ID = "blast-test-" + hex(random.randint(0, sys.maxsize))[2:]
    print("TEST_ID is " + TEST_ID + '\n')

    get_tests()

    #TEST_ID="blast_test-" + secrets.token_urlsafe(6)
    # Instantiates a client

    # Look for cluster
    get_cluster()
    print("\nCluster id is: " + CLUSTER_ID)
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
    time.sleep(15)

    # Start threads
    #  submits
    threading.Thread(target=submit_thread).start()
    #  status
    threading.Thread(target=status_thread).start()
    #  results
    threading.Thread(target=results_thread).start()


#   asn1 diff

if __name__ == "__main__":
    main()
