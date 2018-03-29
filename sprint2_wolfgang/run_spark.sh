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

#spark-submit --master local[4] --class $MAIN_CLASS $MAIN_JAR test.ini
spark-submit --executor-cores 2 --class $MAIN_CLASS $MAIN_JAR test.ini

