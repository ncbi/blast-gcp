#!/bin/bash

#
# in other terminal on same machine call 'nc -lk 9999'
# for cluster with multiple worker node start' ncat -lkv 9999' on the driver
# obtain ncat via 'sudo apt-get install nmap'
#
# on the master-node: 'hadoop fs -copyFromLocal ./todoN.txt todo/todoN.txt'
#

SPARK_HOME="/usr/lib/spark/jars"
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

DEPENDS="$SPARK_HOME/*:."
MAIN_CLASS="GCP_BLAST"
MAIN_JAR="sprint2.jar"

# get rid of eventual leftover compiled classes
rm -rf *.class

echo "compiling java-classes"
javac -Xlint:unchecked -cp $DEPENDS -d . src/*.java
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "compiling java-classes failed"
    exit
fi

echo "packing compiled classes into $MAIN_JAR"
jar cf $MAIN_JAR *class
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "packing compiled classes into $MAIN_JAR failed"
    exit
fi

# get rid of compiled classes after packing them
rm -rf *.class

#spark-submit --files libblastjni.so --master local[4] --class $MAIN_CLASS $MAIN_JAR
#spark-submit --files libblastjni.so --class $MAIN_CLASS $MAIN_JAR
spark-submit --class $MAIN_CLASS $MAIN_JAR
