#!/bin/bash

if [ ! -d "google-pso" ]; then
    echo "Fetching BLAST-GCP status-tracker sub-system"
    gcloud source repos clone google-pso
    pushd google-pso/status-tracker
    make deploy_java
    popd
    echo
fi

clear
echo "compiling java-classes"
mvn -q package

