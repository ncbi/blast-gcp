#!/bin/bash

clear

if [ ! -d "bigdata-interop" ]; then
    git clone https://github.com/GoogleCloudPlatform/bigdata-interop.git
    cd bigdata-interop/pubsub
    mvn package
fi

echo "compiling java-classes"
mvn -q package
exit

SPARK_HOME="/usr/lib/spark/jars/*"
HADOOP_FS_HOME="/usr/lib/hadoop/*"
PUBSUB_JAR="$HOME/bigdata-interop/pubsub/target/*"

