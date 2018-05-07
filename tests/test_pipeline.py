#!/usr/bin/env python3

import atexit
#import base64
import datetime
import getpass
import json
import difflib
import os
import random
#import secrets # python 3.6
import subprocess
import sys
import threading
import time
import uuid

# sudo apt-get update
# sudo apt-get install -y -u python python-dev python3 python3-dev #python3-pip
# sudo apt-get install -y virtualenv
# virtualenv --python python3 env
# source env/bin/activate # "deactivate to deactivate"
# cd env
# pip3 install google-cloud
# pip3 install google-cloud-pubsub
# pip3 install google-cloud-storage


## pip3 install --user --upgrade virtualenv # 2/3 doesn't matter
# sudo pip3 install --upgrade google-cloud-storage

# easy_install --user pip
# pip install --upgrade virtualenv
# pip install --user --upgrade google-cloud
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
BUCKET = None
BUCKET_NAME = ""
TESTS = {}
PUBSUB_CLIENT = None
TOPIC_NAME = ""
SUBSCRIPTION = None
SUBSCRIPTION_PATH = ""
JOB_ID = ""
CONFIG_JSON = ""


def progress(submit=None, results=None):
    if submit is None and results is None:
        progress(" " * 12 + "SUBMIT THREAD", " " * 12 + "RESULTS THREAD")
        progress("-" * 80, "-" * 80)
        return

    if submit is None:
        submit = ""
    else:
        if '\n' in submit:
            lines = submit.split('\n')
            for line in lines:
                progress(submit=line)

    if results is None:
        results = ""
    else:
        if '\n' in results:
            lines = results.split('\n')
            for line in lines:
                progress(results=line)

    print("%-38.38s | %-38.38s" % (submit, results))


def get_cluster():
    global CLUSTER_ID, PROJECT
    user = getpass.getuser()
    #print("user is " + user)
    cmd = [
        'gcloud', 'dataproc', '--project', PROJECT, '--region', 'us-east4',
        'clusters', 'list'
    ]
    results = subprocess.check_output(cmd)
    clusters = results.decode().split('\n')
    for cluster in clusters:
        cluster_name = cluster.split(' ')[0]
        if cluster_name.startswith("blast-dataproc-" + user):
            CLUSTER_ID = cluster_name
            return


def make_pubsub():
    global TOPIC_NAME, SUBSCRIPTION, SUBSCRIPTION_PATH, PUBSUB_CLIENT, TEST_ID
    global PROJECT
    PUBSUB_CLIENT = pubsub.PublisherClient()
    TOPIC_NAME = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT,  # os.getenv('GOOGLE_CLOUD_PROJECT'),
        topic=TEST_ID)

    # Create the topic.
    topic = PUBSUB_CLIENT.create_topic(TOPIC_NAME)
    print('  Topic created: ' + TOPIC_NAME)

    #    print('  Topic created: {}'.format(topic))

    subscriber = pubsub.SubscriberClient()

    SUBSCRIPTION_PATH = subscriber.subscription_path(PROJECT, TEST_ID)
    print("  SUBSCRIPTION_PATH is " + SUBSCRIPTION_PATH)

    SUBSCRIPTION = subscriber.create_subscription(SUBSCRIPTION_PATH,
                                                  TOPIC_NAME)

    #    print('  Subscription created: {}'.format(SUBSCRIPTION))

    return


def make_bucket():
    global STORAGE_CLIENT, TEST_ID, BUCKET, BUCKET_NAME, PROJECT
    STORAGE_CLIENT = storage.Client()
    BUCKET_NAME = TEST_ID
    print('  Creating bucket ' + BUCKET_NAME)
    BUCKET = STORAGE_CLIENT.create_bucket(BUCKET_NAME)
    labels = BUCKET.labels
    labels['owner'] = TEST_ID
    labels['project'] = PROJECT
    labels['description'] = "temporary_test_bucket"
    BUCKET.labels = labels
    BUCKET.update()


def get_tests():
    global TESTS, BUCKET_NAME
    largequery_bucket_name = "blast-largequeries"
    large_bucket = storage.Client().bucket(largequery_bucket_name)
    largequery_bucket_name = "gs://" + largequery_bucket_name

    # gsutil mb -p ncbi-sandbox-blast -c regional -l us-east4 gs://blast-largequeries
    #gsutil mb -p ncbi-sandbox-blast -c regional -l us-east4 gs://blast-builds
    #echo '{ "rule": [ { "action": {"type": "Delete"}, "condition": {"age": 1} } ] }' >> ^rule.json
    # echo '{ "description": "temp_storage", "owner" : "$USER" }' > labels.json
    #gsutil lifecycle set rule.json gs://blast-builds
    #gsutil label set labels.json gs://blast-builds

    test_blobs = os.listdir('queries')
    random.shuffle(test_blobs)

    for test_file in test_blobs:
        test_file = 'queries/' + test_file
        with open(test_file) as fin:
            read_data = fin.read()

        j = json.loads(read_data)
        j['orig_RID'] = j['RID']
        j['pubsub_submit_time'] = "TBD"
        j['RID'] = TEST_ID + '-' + j['RID']
        j['query_url']=''
        j['blast_params']['task']=j['blast_params']['program']
        j['result_bucket_name']=BUCKET_NAME
        del j['query_url']
        # Randomly put 1% of queries in gs bucket instead
        if random.randrange(0, 100) < 20:
            print("Using out of band query")

            #objname = j['RID'] + "-" + str(uuid.uuid4())
            objname = 'query-' + ('%09d.txt' % random.randrange(1, 1000000000))

            blob = large_bucket.blob(objname)
            blob.upload_from_string(j['blast_params']['queries'][0])
            del j['blast_params']['queries']

            url = largequery_bucket_name + "/" + objname

            j['query_seq'] = ''
            del j['query_seq']
            j['query_url'] = url
            #print(json.dumps(j, indent=4, sort_keys=True))

        TESTS[j['RID']] = j
    print("Loaded " + str(len(TESTS)) + " tests")


def publish(jdict):
    global TOPIC_NAME, PUBSUB_CLIENT
    msg = json.dumps(jdict, indent=4, sort_keys=True).encode()
    PUBSUB_CLIENT.publish(TOPIC_NAME, msg)


def make_json():
    global BUCKET_NAME, SUBSCRIPTION_PATH, TEST_ID, PROJECT, CLUSTER_ID
    j = {}
    if False:
        j['log_start'] = 'false'
        j['log_done'] = 'false'
        j['result_bucket'] = BUCKET_NAME
        j['status_bucket'] = BUCKET_NAME
        j['subscript_id'] = TEST_ID  # SUBSCRIPTION_PATH
        j['log_request'] = 'true'
        j['log_worker_shift'] = 'false'
        j['batch_duration'] = 2
        j['trigger_port'] = 0
        j['log_port'] = 0
        j['log_partition_prep'] = "true"
        j['top_n'] = 100
        j['num_db_partitions'] = 886
        j['num_executors'] = 180
        j['num_executor_cores'] = 1
        j['project_id'] = PROJECT
    else:
        j['spark'] = {}
        j['spark']['with_locality'] = False
        j['spark']['num_executors'] = 180
        j['spark']['num_executor_cores'] = 1
        j['blastjni'] = {}
        j['blastjni']['db'] = {}
        j['blastjni']['db']['db_bucket'] = 'nt_50mb_chunks'
        j['blastjni']['db']['num_db_partitions'] = 886
        j['blastjni']['top_n'] = 100
        j['source'] = {}
        j['source']['pubsub'] = {}
        j['source']['pubsub']['project_id'] = PROJECT
        j['source']['pubsub']['subscript_id'] = TEST_ID
        j['result'] = {}
        j['result']['asn1'] = {}
        j['result']['asn1']['bucket'] = BUCKET_NAME
        j['result']['status'] = {}
        j['result']['status']['bucket'] = BUCKET_NAME
        j['log'] = {}
        j['log']['log_port'] = 0
    return json.dumps(j, indent=4, separators=(',', ' : '), sort_keys=True)


def submit_application(config):
    global CLUSTER_ID, TEST_ID, JOB_ID, CONFIG_JSON, PROJECT
    CONFIG_JSON = TEST_ID + ".json"
    fout = open(CONFIG_JSON, "w")
    fout.write(config)
    fout.close()

    # spark-submit --master yarn
    # --jars /home/vartanianmh/bigdata-interop/pubsub/target/spark-pubsub-0.1.0-SNAPSHOT-shaded.jar
    # --class gov.nih.nlm.ncbi.blastjni.BLAST_MAIN
    # ./target/sparkblast-1-jar-with-dependencies.jar
    # foo.json

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
    # TODO: magic pubsub jar should be in git or make_jars.sh
    cmd.append(
        "/home/vartanianmh/bigdata-interop/pubsub/target/spark-pubsub-0.1.0-SNAPSHOT-shaded.jar"
        + "," + "../pipeline/target/sparkblast-1-jar-with-dependencies.jar")
    cmd.append("--project")
    cmd.append(PROJECT)
    cmd.append("--files")
    cmd.append("../pipeline/libblastjni.so," + CONFIG_JSON)
    cmd.append("--region")
    cmd.append("us-east4")
    cmd.append("--max-failures-per-hour")
    cmd.append("1")
    cmd.append("--")
    cmd.append(CONFIG_JSON)
    print("  cmd is : " + ' '.join(cmd))
    #JOB_ID = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode()
    #JOB_ID = JOB_ID.replace("\n", "\n        ")
    #print("JOB_ID is    " + JOB_ID)


def submit_thread():
    global TESTS
    progress(submit=("Submit thread started: " + str(len(TESTS)) + " tests"))
    while True:
        tests = list(TESTS.keys())
        random.shuffle(tests)
        for test in tests[0:3]:
            # Emulate 1..10 submissions a second
            #time.sleep(random.randrange(0, 100) / 1000)
            TESTS[test]['pubsub_submit_time'] = time.time()
            #            TESTS[test]['pubsub_submit_time'] = datetime.datetime.now().isoformat()
            publish(TESTS[test])
            progress(submit="  Submitted " + TESTS[test]['orig_RID'])
            time.sleep(random.randrange(5, 10)/10)
        progress(submit="Enough tests submitted, taking a break.")
        time.sleep(60)


def results_thread():
    global STORAGE_CLIENT, BUCKET, BUCKET_NAME, TESTS
    while True:
        time.sleep(1)
        anything = False
        for blob in BUCKET.list_blobs():  # prefix='output',delimiter='/'):
            anything = True
            # gs://blast-test-$USER/output/RID/seq-annot.asn/
            # gs://blast-test-$USER/status/RID/status.txt
            parts = blob.name.split('/')
            if parts[0] == 'status':
                # TODO: Check Job Orchestration status
                continue
            # progress(results=str(parts))
            rid = parts[1]

            result = TESTS[rid]
            origrid = result['orig_RID']
            os.makedirs(name='results', exist_ok=True)
            fname = "results/" + origrid + "." + parts[2]

            blob.download_to_filename(fname)
            dtend = blob.time_created
            dtend = dtend.replace(tzinfo=None)
            BUCKET.delete_blob(blob.name)

            dtstart = datetime.datetime.utcfromtimestamp(
                result['pubsub_submit_time'])
            elapsed = dtend - dtstart
            progress(results="%s took %6.2f seconds" % (
                origrid, elapsed.total_seconds()))

            cmd = [
                "asntool", "-m", "/am/ncbiapdata/asn/asn.all", "-t",
                "Seq-annot", "-d", fname, "-p", fname + ".txt"
            ]
            #print (cmd)
            subprocess.check_output(cmd)

            with open(fname + ".txt") as fnew:
                fnewlines=fnew.readlines()
            with open("expected/" + origrid + "." + parts[2] + ".txt") as fexpected:
                fexpectedlines=fexpected.readlines()

            if fnewlines!=fexpectedlines:
                print("Files differ for " + origrid)
                diff=difflib.ndiff(fnewlines, fexpectedlines)
                print(diff)

        if not anything:
            progress(results="No objects in bucket")
            time.sleep(20)


def cleanup():
    global PUBSUB_CLIENT, TOPIC_NAME, BUCKET, BUCKET_NAME
    global STORAGE_CLIENT, CONFIG_JSON, SUBSCRIPTION, SUBSCRIPTION_PATH
    print("*** Cleaning up... ***")
    if CONFIG_JSON:
        os.remove(CONFIG_JSON)
    if TOPIC_NAME:
        print("  Removing TOPIC: " + TOPIC_NAME)
        PUBSUB_CLIENT = pubsub.PublisherClient()
        PUBSUB_CLIENT.delete_topic(TOPIC_NAME)
    if SUBSCRIPTION:
        print("  Removing SUBSCRIPTION: " + SUBSCRIPTION_PATH)
        subscriber_client = pubsub.SubscriberClient()
        subscriber_client.delete_subscription(SUBSCRIPTION_PATH)
    if BUCKET:
        for blob in BUCKET.list_blobs():
            print("    Deleting " + blob.name + " from bucket " + BUCKET_NAME)
            BUCKET.delete_blob(blob.name)
        print("  Removing BUCKET: " + BUCKET_NAME)
        bucket = STORAGE_CLIENT.get_bucket(BUCKET_NAME)
        bucket.delete()
    # TODO: Kill application?


#        cmd = "gsutil -m rm -r gs://" + BUCKET
#        subprocess.check_output(cmd)


def main():
    global TEST_ID, CLUSTER_ID
    # register atexit
    atexit.register(cleanup)
#    TEST_ID = "blast-test-" + hex(random.randint(0, sys.maxsize))[2:]
#    TEST_ID = str(uuid.uuid4())
    TEST_ID = "blast-test-vartanianmh"
    print("TEST_ID is " + TEST_ID)

    # Create output bucket
    make_bucket()

    get_tests()

    #TEST_ID="blast_test-" + secrets.token_urlsafe(6)
    # Instantiates a client

    # Look for cluster
    get_cluster()
    print("Cluster id is: " + CLUSTER_ID)
    # Start if not found?

    # Create pubsub queue
    make_pubsub()

    # configure json.ini
    config = make_json()
    #print("JSON config is " + config)
    #print(config)

    print()
    print("PubSub subscriptions created")
    print("Cloud Storage bucket created")
    print()
    #submit_application(config)
    print()
    print(" " * 20, "*** Start Spark Streaming Job now ***")
    time.sleep(40)
    print()
    time.sleep(1)

    # Start threads
    #  submits
    progress(None, None)
    threading.Thread(target=submit_thread).start()
    #  status
    #threading.Thread(target=status_thread).start()
    #  results
    threading.Thread(target=results_thread).start()


#   asn1 diff

if __name__ == "__main__":
    main()
