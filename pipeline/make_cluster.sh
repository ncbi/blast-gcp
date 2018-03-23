#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

gcloud dataproc --region us-east4 \
    clusters create cluster-$USER \
    --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --project ncbi-sandbox-blast \
    --labels owner=$USER \
    --region us-east4 \
    --zone   us-east4-b \
    --image-version 1.2 \
    --initialization-action-timeout 30m \
    --initialization-actions \
    "$PIPELINEBUCKET/scipts/cluster_initialize.sh" \
    --tags ${USER}-dataproc-cluster-$(date +%Y%m%d-%H%M%S) \
    --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4

exit 0
#gcloud auth login (copy/paste from web)
#gcloud dataproc clusters list  --region=us-east4
# gcloud dataproc jobs list
# gcloud dataproc jobs submit spark --cluster XXX --jar foo.jar arg1 arg2
# gcloud dataproc jobs submit spark --cluster cluster-blast-vartanianmh --class org.apache.spark.examples.SparkPi --region=us-east4

# gcloud dataproc jobs submit spark --cluster cluster-blast-vartanianmh --class org.apache.spark.examples.SparkPi --jars file:///usr/lib/spark/examples/jars/spark-examples.jar  --region=us-east4 --max-failures-per-hour 2

# gcloud dataproc clusters diagnose cluster-name
#--zone "" ?
#--max-age=8h \
#--single-node

