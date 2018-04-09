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

SPARK_BLAST_CLASS="gov.nih.nlm.ncbi.blastjni.GCP_BLAST"
SPARK_BLAST_JAR="spark_blast.jar"
SPARK_BLAST_INI="test.ini"
#PUBSUB_JAR1="$HOME/bigdata-interop/pubsub/target/spark-pubsub-0.1.0-SNAPSHOT.jar"
#PUBSUB_JAR2="$HOME/bigdata-interop/pubsub/target/spark-pubsub-0.1.0-SNAPSHOT-shaded.jar"

#
# on google-cluster:
#   --num-executers X   : X should match the number or worker-nodes
#   --executor-cores Y  : Y should match the number of vCPU's per worker-node 
#

#spark-submit --master local[4] --class $SPARK_BLAST_CLASS $SPARK_BLAST_JAR $SPARK_BLAST_INI
spark-submit --master yarn --class $SPARK_BLAST_CLASS $SPARK_BLAST_JAR $SPARK_BLAST_INI

