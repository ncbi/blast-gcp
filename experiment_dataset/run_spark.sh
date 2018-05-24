#!/bin/bash

#
# in another terminal: 'nc -lk 10011' to see the log-output
# in another terminal: 'ncat -lk 10012' to trigger jobs
#
# ports can be changed via test.ini
#
# obtain ncat via 'sudo apt-get install nmap'
#
# on the master-node: 'hadoop fs -ls results' to see produced rdd's
#

SPARK_BLAST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_DRIVER"
SPARK_BLAST_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
SPARK_BLAST_INI="ini.json"

#
# on google-cluster:
#   --num-executers X   : X should match the number or worker-nodes
#   --executor-cores Y  : Y should match the number of vCPU's per worker-node 
#

#    /user/"$USER"/requests/*json \
hadoop fs -rm -f \
    /user/"$USER"/results/hsps/* \
    /user/"$USER"/results/*
hadoop fs -mkdir -p /user/"$USER"/requests/
hadoop fs -mkdir -p /user/"$USER"/results/hsps

cp ../lib_builder/libblastjni.so .

spark-submit --master yarn --class $SPARK_BLAST_CLASS $SPARK_BLAST_JAR $SPARK_BLAST_INI


