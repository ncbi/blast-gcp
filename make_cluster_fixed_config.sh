#!/bin/bash -x
# make_cluster_fixed_config.sh: Configure a spark cluster to search nt optimally
#
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Wed 08 Aug 2018 09:42:15 AM EDT

set -euo pipefail

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

# NT takes about 49GB, NR 66GB, so n1-standard-32's 120GB adequate if running
# both DBs simultaenously
# n1-highcpu-96 has 86 GB
# Workers are ~13GB: 5G for Spark/Linux and 1GB/JVM
#DB_SPACE=166
MASTER=n1-standard-4
WORKER=n1-standard-64
# Note that GCP is soon going to default to 1TB PD disks for throughput, but
# since we just load everything into kernel cache at startup, we're
# not as sensitive to this as most Hadoop applications.
DISK_PER_MASTER=400 # For test data
DISK_PER_WORKER=400 # For DBs and logs
NUM_WORKERS=16
PREEMPT_WORKERS=0

gcloud beta dataproc \
    clusters create "blast-dataproc-$USER-big" \
    --master-machine-type $MASTER \
        --master-boot-disk-size $DISK_PER_MASTER \
    --num-workers $NUM_WORKERS \
        --worker-boot-disk-size $DISK_PER_WORKER \
    --worker-machine-type $WORKER \
    --num-preemptible-workers $PREEMPT_WORKERS \
        --preemptible-worker-boot-disk-size $DISK_PER_WORKER \
    --scopes cloud-platform \
    --project ncbi-sandbox-blast \
    --max-age=6h \
    --labels owner="$USER" \
    --region us-east4 \
    --zone   us-east4-c \
    --properties dataproc:dataproc.monitoring.stackdriver.enable=true,dataproc:dataproc.logging.stackdriver.enable=true, \
    --initialization-action-timeout 30m \
    --initialization-actions \
    $PIPELINEBUCKET/scripts/cluster_initialize.sh,$PIPELINEBUCKET/scripts/ganglia.sh \
    --tags "blast-dataproc-$USER-$(date +%Y%m%d-%H%M%S)" \
    --bucket "${USER}-test"

