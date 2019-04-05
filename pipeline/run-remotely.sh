#!/bin/bash
# run-remotely.sh: Script to launch the simple spark app at GCP without logging into the master.
# Assumes cluster started with make_cluster_fixed_config.sh
# This script is experimental

echo "NOT READY FOR USAGE"
exit 1

DB=${1:-"nr"}
CONFIG=$DB-ini.json
CLUSTER_ID=blast-dataproc-${USER}
MAIN_CLASS=gov.nih.nlm.ncbi.blastjni.BC_MAIN
JAR=$(find target -type f -name "*-with-dependencies.jar")
LOG_CONF="--driver-java-options=-Dlog4j.configuration=file:log4j.properties"
GCP_REGION=us-east4

if [[ $DB != "nr" && $DB != "nt" ]] ; then
    echo "Usage: $0 [nr|nt]"
    exit 1
fi

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .

if [ ! -z "$CLUSTER_ID" ] ; then
    set -x
    gcloud dataproc jobs submit spark --jars $JAR \
        --class ${MAIN_CLASS} \
        --cluster ${CLUSTER_ID} \
        --region ${GCP_REGION} \
        --driver-log-levels=root=INFO \
        --files=libblastjni.so,$CONFIG,${DB}-searches.txt -- $CONFIG ${DB}-searches.txt
fi
