#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

usage="
usage: $0
Required environment variables:
    project
    region
    zone
    deploy_id
    blast_dataproc_cluster_name
    blast_dataproc_cluster_max_age
"

[ $# -eq 0 ] || { echo ${usage} && exit 1; }

checkvar=${project?"${usage}"}
checkvar=${region?"${usage}"}
checkvar=${zone?"${usage}"}
checkvar=${deploy_id?"${usage}"}
checkvar=${blast_dataproc_cluster_name?"${usage}"}
checkvar=${blast_dataproc_cluster_max_age?"${usage}"}

gcloud beta dataproc clusters create ${blast_dataproc_cluster_name} \
    --master-machine-type n1-standard-4 --master-boot-disk-size 100 \
    --num-workers 2 --worker-boot-disk-size 171 --worker-machine-type custom-64-151552  \
    --num-preemptible-workers 0 --preemptible-worker-boot-disk-size 171 --scopes cloud-platform \
    --project ${project} \
    --labels owner=${USER},deploy_id=${deploy_id} \
    --region ${region} \
    --zone ${zone} \
    --max-age=${blast_dataproc_cluster_max_age} \
    --tags ${blast_dataproc_cluster_name} \
    --image-version 1.2 \
    --initialization-action-timeout 30m \
    --initialization-actions gs://blastgcp-pipeline-test/scripts/cluster_initialize.sh \
    --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4

