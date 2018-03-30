#!/bin/bash

#
# in another terminal: 'ncat -lk 10011' to see the log-output
# in another terminal: 'ncat -lk 10012' to trigger jobs
#
# ports can be changed via test.ini
#
# obtain ncat via 'sudo apt-get install nmap'
#
# on the master-node: 'hadoop fs -ls results' to see produced rdd's
#

MAIN_CLASS="GCP_BLAST"
MAIN_JAR="sprint2.jar"

#
# on google-cluster:
#   --num-executers X   : X should match the number or worker-nodes
#   --executor-cores Y  : Y should match the number of vCPU's per worker-node 
#

#spark-submit --master local[4] --class $MAIN_CLASS $MAIN_JAR test.ini
spark-submit --master yarn --num-executors 2 --executor-cores 4 --class $MAIN_CLASS $MAIN_JAR test.ini

