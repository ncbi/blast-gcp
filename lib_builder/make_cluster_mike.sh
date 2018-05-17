#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

# Note: 100GB enough for NT
# Larger nodes have faster network, and faster startup
# Takes an n1-standard-32 about 11 minutes to copy 50GB NT
# in parallel. No improvement seen by tar'ing them all up into one object to
# download.
#
# FIX: nr is 196GB+
#
# IFF blast is memory bandwidth bound, n1-standard-8 appears to have similar
# single core memory bandwidth as n1-standard-32. Will need to benchmark
# further to determine if more or fatter nodes are optimal.
#
# Pricing
# ~13% higher in NoVA (us-east4) versus
#                South Carolina (us-east1) /
#                Iowa (us-central1)
# 96 core not available in us-east4, us-west1-c, us-east1-b
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
# plus Dataproc surcharge ($0.01/vCPU): ** Half of our expense?!? **
# plus provisioned storage ($0.044/ GB month)
# minus sustained use (25-30%), committed use (~40-60%), and preemptible (80%)
#
# Memory must be 0.9-6.5GB/vCPU
#
# If we hypothetically require 1000 vCPUs to meet SLA,
# cheaper to have 11 96-core machines, with 11 disks?
#
# But at least two workers must be non-preemptible (5X $), so need
# to balance disk costs versus workers.
#
# GCP' pricing calculator:
#  32 standard-32: $17,347/month
#  16 highcpu-64:  $14,932/month
#  32 highcpu-32:  $15,113/month
#  64 highcpu-16:  $14,079/month
#  64 highcpu-16:  $13,607/month (limited disk)
# 256 highcpu-4:   $15,581/month
#  16 highmem-64:  $19,535/month (limited disk, all ramdisk)
# Ex: $11,666/month (12 std-64)
#
# Dataflow pricing is:
#    $0.07/vCPU hour
#    $0.003/GB hour (so at least another $0.01/vCPU hour)

# 886 partitions:
# 16 each to 2 workers
# 854/16 amongst 54 preemptible workers

# Need 50+GB for NT DB to be resident in kernel page cache.
# Figure Spark/Hadoop/OS/JVMs take ~4GB, so need
# at least 54GB RAM per node. GCE won't allow less than
# 0.9GB per CPU, so at least 57.6GB for 64 cores, which is n1-highcpu-64

# NIC gets 2gbit/vCPU, 16gbit max. Single core max ~8.5gbit.

# TODO:
# YARN, 1 core? Dataproc has /etc/.../spark-defaults.conf set as:
# # User-supplied properties.
#  Thu Apr 26 11:46:31 UTC 2018
#  spark.executor.cores=8
#  spark.executor.memory=4655m
#  spark.driver.memory=7680m
#  spark.driver.maxResultSize=3840m
#  spark.yarn.am.memory=640m

# check Spark Web UI

# standard-16 and highcpu-64 have >58GB
# Likely need ~2 GB/vCPU: custom-8-16384?

# ~$15/hour for 56 node cluster
# ~$21/hour for 16 X 64
#    --worker-machine-type custom-64-71680 \
gcloud beta dataproc --region us-east4 \
    clusters create blast-dataproc-"$USER" \
    --master-machine-type n1-standard-8 \
        --master-boot-disk-size 100 \
    --num-workers 2 \
        --worker-boot-disk-size 100 \
    --worker-machine-type n1-standard-32 \
    --num-preemptible-workers 4 \
        --preemptible-worker-boot-disk-size 100 \
    --scopes cloud-platform \
    --project ncbi-sandbox-blast \
    --labels owner="$USER" \
    --region us-east4 \
    --zone   us-east4-b \
    --max-age=8h \
    --image-version 1.2 \
    --initialization-action-timeout 30m \
    --initialization-actions \
    "$PIPELINEBUCKET/scripts/cluster_initialize_mike.sh" \
    --tags blast-dataproc-"$USER"-"$(date +%Y%m%d-%H%M%S)" \
    --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4

# dataproc-3bd9289a... has 15 day deletion lifecycle

# TODO: Scopes: storage-rw, pubsub, bigtable.admin.table, bigtabl.data,
# devstorage.full_control,
exit 0
# gcloud dataproc clusters diagnose cluster-name
#--single-node

