#!/bin/bash



if [ ! -d "bigdata-interop" ]; then
    echo "compiling pubsub library"
    git clone https://github.com/GoogleCloudPlatform/bigdata-interop.git
    pushd bigdata-interop/pubsub
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
    mvn -q package
    popd
fi

echo "compiling java-classes"
mvn -q package
exit

SPARK_HOME="/usr/lib/spark/jars/*"
HADOOP_FS_HOME="/usr/lib/hadoop/*"
PUBSUB_JAR="$HOME/bigdata-interop/pubsub/target/*"

