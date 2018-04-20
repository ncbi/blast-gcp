#!/usr/bin/env python

import json
import random
import base64
#import secrets # python 3.6
# pip3 install --user --upgrade google-cloud-storage
# pip3 install --user --upgrade google-cloud-pubsub
from google.cloud import pubsub
from google.cloud import storage
#from google.cloud import pubsub, storage
#from gcloud import pubsub, storage

session_id="blast_test-" + hex(random.randint(0,sys.maxsize))[2:]
#session_id="blast_test-" + secrets.token_urlsafe(6)
# Instantiates a client
storage=storage.Client()
publisher_client = pubsub.PublisherClient()
#subscriber = pusub_v1.SubscriberClient()

#topic#_path = publisher.topic_path(
topic_path= 'projects/{project_id}/topics/{topic}'.format(
        project_id="ncbi-sandbox-blast", # os.getenv('GOOGLE_CLOUD_PROJECT'),
        topic=session_id)
# Create the topic.
topic = publisher_client.create_topic(topic_path)

#topic=publisher.get_topic(topic_path)
print('Topic created: {}'.format(topic))

publisher_client.publish(topic_path, b'test')

test_request_bucket=storage.get_bucket("blast-test-requests")

test_blobs=[]
for test in test_request_bucket.list_blobs():
    test_blobs.append(test)

shuffle(test_blobs)

for test in test_blobs[0:5]:
    print (test)
    foo=test.download_as_string()
    j=json.loads(foo.decode())
    print ("RID was " + j['RID'])
    j['RID']=session_id + j['RID']
    print ("RID  is " + j['RID'])
    msg=json.dumps(j,indent=4).encode()
    print (msg)
    response=publisher_client.publish(topic_path, msg)
    # TODO: jitter


#publisher_client.delete_topic(topic_path)

