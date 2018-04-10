#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

# Note: 100GB enough for NT
# Larger nodes have faster network, and faster startup
# Takes an n1-standard-32 about 11 minutes to copy 50GB NT
# in parallel. No improvement seen by tar'ing them all up into one object to
# downloaed.
#
# IFF blast is memory bandwidth bound, n1-standard-8 appears to have similar
# single core memory bandwidth as n1-standard-32. Will need to benchmark
# further to determine if more or fatter nodes are optimal.
#
# Pricing
# ~13% higher in NoVA (us-east4) versus
#                South Carolina (us-east1) /
#                Iowa (us-central1)
# n1-standard-N: N=1,2,...64,96
#   $0.0535/$0.0107 per hour (normal/preemptible)
#   N vCPU
#   Nx3.75GB
# n1-highcpu-N: N=2,4,64,96
#   $0.0406/$.00802 per hour ("") (so ~25% cheaper)
#   Nx0.90GB
# n1-highmem-N: N=2,4,64,96
#   $0.0674/$0.0134 per hour ("") (so ~25% more expensive)
#
# plus DataProc surcharge ($0.01/vCPU)
# plus provisioned storage ($0.044/ GB month)
# minus sustained use (25-30%) and committed use (~50-60%)
gcloud dataproc --region us-east4 \
    clusters create cluster-$USER-$(date +%Y%m%d) \
    --master-machine-type n1-standard-8 \
        --master-boot-disk-size 100 \
    --num-workers 2 \
    --worker-machine-type n1-standard-32 \
        --worker-boot-disk-size 100 \
    --num-preemptible-workers 2 \
        --preemptible-worker-boot-disk-size 100 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --project ncbi-sandbox-blast \
    --labels owner=$USER \
    --region us-east4 \
    --zone   us-east4-b \
    --image-version 1.2 \
    --initialization-action-timeout 30m \
    --initialization-actions \
    "$PIPELINEBUCKET/scripts/cluster_initialize.sh" \
    --tags ${USER}-dataproc-cluster-$(date +%Y%m%d-%H%M%S) \
    --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4

exit 0
# gcloud dataproc clusters diagnose cluster-name
# beta: --max-age=8h
#--single-node

