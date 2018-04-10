#!/bin/bash

tput reset

SPARK_HOME="/usr/lib/spark/jars/*"
PUBSUB_JAR="$HOME/bigdata-interop/pubsub/target/*"
SPARK_BLAST_DEPEND="$SPARK_HOME:$PUBSUB_JAR:."
SPARK_BLAST_SRC="src/*.java"
SPARK_BLAST_JAR="spark_blast.jar"
GOOGLE_JARS="$HOME/google_cloud_jars/*"
GOOGLE_PUBSUB_JAVA="$HOME/google-cloud-java/google-cloud-pubsub/src/main/java"

echo "compiling java-classes"

# get rid of compiled classes and before rebuilding them
rm -rf ./gov $SPARK_BLAST_JAR

javac -Xlint:all -Xlint:-path -Xlint:-serial -cp $SPARK_BLAST_DEPEND -d . $SPARK_BLAST_SRC
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "compiling java-classes failed"
    exit
fi

#echo "compiling pubsub client"
#pushd $GOOGLE_PUBSUB_JAVA
#javac -Xlint:unchecked -d "$HOME/blast-gcp/sprint2_wolfgang"  com/google/cloud/pubsub/v1/*.java
#retval=$?
#popd
#if [[ $retval -ne 0 ]]; then
#    echo "compiling pubsub client failed"
#    exit
#fi

echo "packing compiled classes into $SPARK_BLAST_JAR"
#jar cf $SPARK_BLAST_JAR *class $PUBSUB_JAR $GOOGLE_JARS
jar cf $SPARK_BLAST_JAR ./gov/nih/nlm/ncbi/blastjni/*class
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "packing compiled classes into $SPARK_BLAST_JAR failed"
    exit
fi

# get rid of compiled classes after packing them
rm -rf ./gov

echo "java build of blast on spark: success"

