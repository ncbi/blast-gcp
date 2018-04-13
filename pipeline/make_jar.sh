#!/bin/bash

clear
echo "compiling java-classes"
mvn -q package
exit

SPARK_HOME="/usr/lib/spark/jars/*"
HADOOP_FS_HOME="/usr/lib/hadoop/*"
PUBSUB_JAR="$HOME/bigdata-interop/pubsub/target/*"

