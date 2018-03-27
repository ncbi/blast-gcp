#!/bin/bash

#
# in other terminal on same machine call 'nc -lk 9999'
# for cluster with multiple worker node start' ncat -lkv 9999' on the driver
# obtain ncat via 'sudo apt-get install nmap'
#

SPARK_HOME="/usr/lib/spark/jars"
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

DEPENDS="$SPARK_HOME/*:."
MAIN_CLASS="GCP_BLAST"
MAIN_JAR="sprint2.jar"

# get rid of eventual leftover compiled classes
rm -rf *.class

echo "compiling java-classes"
TEST="BlastJNI"
javac -Xlint:unchecked -cp $DEPENDS  -d . src_test/$TEST.java
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "compiling java-classes failed"
    exit
fi

CURDIR=`pwd`
JAVA_LIB_PATH="-Djava.library.path=$CURDIR"
JAVA_CLASS_PATH="-cp $DEPENDS"
java $JAVA_LIB_PATH $JAVA_CLASS_PATH $TEST
