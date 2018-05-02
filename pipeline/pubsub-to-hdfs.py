#!/usr/bin/env python3

import datetime
import getpass
import json
import difflib
import os
import random
import subprocess
import sys
import threading
import time
import uuid

from google.cloud import pubsub_v1

HADOOPTMP="/tmp/"
HADOOPDEST="/user/spark/queries/"

def create_subscription(project, topic_name, subscription_name):
    """Create a new pull subscription on the given topic."""
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project, topic_name)
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    subscription = subscriber.create_subscription(
        subscription_path, topic_path)

    print('Subscription created: {}'.format(subscription))

def receive_messages(project, subscription_name):
    global HADOOPTMP, HADOOPDEST

    """Receives messages from a pull subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        global HADOOPTMP, HADOOPDEST
        print('Received message: {}'.format(message))
        #print(type(message))
        #print(dir(message))
        print(message.data)
        s=message.data.decode()
        randname='query-' + str(uuid.uuid4()) + '.json'
        with open(randname,'w') as fout:
            fout.write(s)
        cmd=['hadoop', 'fs', '-copyFromLocal', randname,  HADOOPTMP + randname]
        print (cmd)
        subprocess.run(cmd)
        cmd=['hadoop', 'fs', '-mv', HADOOPTMP + randname , HADOOPDEST + randname]
        print (cmd)
        subprocess.run(cmd)

        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)

def main():
    project = "ncbi-sandbox-blast"
    topic= "blast-test-vartanianmh"

    subscriber = pubsub_v1.SubscriberClient()
    #project=subscriber.project_path(project)
    user = getpass.getuser()
    mysub=user + '-' + str(random.randrange(0,5000))
    create_subscription(project, topic, mysub)
    receive_messages(project, mysub) # Never returns
    return

if __name__ == "__main__":
    main()
