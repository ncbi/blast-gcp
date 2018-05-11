#!/bin/bash

#distro=$(grep Debian /etc/os-release | wc -l)
#if [ "$distro" -ne 0 ]; then
#    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
#    export PATH="$JAVA_HOME/bin:$PATH"
#else
#    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
#fi


#if [ ! -d "bigdata-interop/pubsub/target" ]; then
#    echo "compiling pubsub library"
#    git clone https://github.com/GoogleCloudPlatform/bigdata-interop.git
#    pushd bigdata-interop/pubsub
#    mvn -q package
#    popd
#    echo
#fi

if [ ! -d "google-pso" ]; then
    echo "Fetching BLAST-GCP status-tracker sub-system"
    gcloud source repos clone google-pso
    pushd google-pso/status-tracker
    make deploy_java
    popd
    echo
fi

echo "compiling java-classes"
#clear
#mvn package | grep -v INFO
mvn -q package
exit

