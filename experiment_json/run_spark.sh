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

SPARK_BLAST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_MAIN"
SPARK_BLAST_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
SPARK_BLAST_INI="ini.json"

#
# on google-cluster:
#   --num-executers X   : X should match the number or worker-nodes
#   --executor-cores Y  : Y should match the number of vCPU's per worker-node - 1
#

spark-submit --master yarn --class $SPARK_BLAST_CLASS $SPARK_BLAST_JAR $SPARK_BLAST_INI

#gcloud dataproc jobs submit spark --cluster wblast --class $SPARK_BLAST_CLASS --jars $PUBSUB_JAR,$SPAKR_BLAST_JAR --project ncbi-sandbox-blast --region us-east4 -- $SPARK_BLAST_INI

