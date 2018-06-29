#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

# Tunables
NUM_CORES=32

# Where to find startup script
PIPELINEBUCKET="gs://blastgcp-pipeline-test"

function config()
{
    NUM_WORKERS=$(((NUM_CORES - 1 + CORES_PER_WORKER) / CORES_PER_WORKER))
    if [[ "$NUM_WORKERS" -lt 2 ]]; then
        NUM_WORKERS=2
    fi

    # NT takes about 49GB, NR 116GB
    DB_SPACE=166
    DB_SPACE_PER_WORKER=$((DB_SPACE / NUM_WORKERS))

    CORES_PER_MASTER=4
    RAM_PER_MASTER=$((CORES_PER_MASTER * 4 * 1024))
    DISK_PER_MASTER=400 # For test data
    MASTER=custom-"$CORES_PER_MASTER-$RAM_PER_MASTER"
    # FIX: Override with standard types
    MASTER=n1-standard-4

    # DataProc is going to only start 2 executors per worker.
    # They'll each get ~35% of RAM
    # DataProc install takes around 5GB of disk, minimum 50 for logs
    DISK_PER_WORKER=$((DB_SPACE_PER_WORKER + 50))

    # FIX: Until we can guarantee DB pinning or implement LRU:
    #DISK_PER_WORKER=$((DB_SPACE + 5))

    # JVMS ~ 2GB per executor, plus 2GB overhead, plus cache for DB
    RAM_PER_WORKER=$(( (2 * CORES_PER_WORKER + 2 + DB_SPACE_PER_WORKER)*1024 ))

    MIN_RAM=$((CORES_PER_WORKER * 921)) # 1024 * 0.6
    MAX_RAM=$((CORES_PER_WORKER * 6656)) # 1024 * 6.5

    if [[ "$RAM_PER_WORKER" -lt "$MIN_RAM" ]]; then
        echo "RAM $RAM_PER_WORKER must be >= $MIN_RAM"
        RAM_PER_WORKER=$MIN_RAM
    fi

    if [[ "$RAM_PER_WORKER" -gt "$MAX_RAM" ]]; then
        echo "RAM $RAM_PER_WORKER must be <= $MAX_RAM"
        RAM_PER_WORKER=$MAX_RAM
    fi

    # FIX: Override with standard types: high-cpu ~3% cheaper than custom
    # But if we need 1GB/core, not going to use highcpu
    WORKER=custom-"$CORES_PER_WORKER-$RAM_PER_WORKER"

    PREEMPT_WORKERS=$((NUM_WORKERS - 2))
    if [[ $PREEMPT_WORKERS -lt 0 ]]; then
        PREEMPT_WORKERS=0
    fi

    # DataProc costs 1c/vCPU
    COST=0 # Units are cents/hour
    COST=$(bc -l <<< "$COST + ($CORES_PER_WORKER * $NUM_WORKERS + $CORES_PER_MASTER)/100" )
    # Custom vCPU is $0.037 in Nova, $0.007469 preempt
    # Assume sustained use
    COST=$(bc -l <<< "$COST + $CORES_PER_MASTER * 0.037364*0.7" )
    # RAM is $.005/GB hour, $.001 preempt
    COST=$(bc -l <<< "$COST + ($RAM_PER_MASTER/1024) * 0.005008*0.7" )

    # 2 normal workers for HDFS replication
    COST=$(bc -l <<< "$COST + 2 * 0.037364*.6" )
    COST=$(bc -l <<< "$COST + 2 * ($RAM_PER_WORKER/1024) * .005008*0.7" )

    # Preemptive workers
    COST=$(bc -l <<< "$COST + $CORES_PER_WORKER * $PREEMPT_WORKERS * 0.007469" )
    COST=$(bc -l <<< "$COST + $PREEMPT_WORKERS * ($RAM_PER_WORKER/1024) * 0.001006" )

    # Persistent disks: Standard provisioned is $.044/GB month
    COST=$(bc -l <<< "$COST + $DISK_PER_MASTER * .044 / 720" )
    COST=$(bc -l <<< "$COST + $NUM_WORKERS * $DISK_PER_WORKER * 0.044 /720" )

    # Convert from pennies to dollars
    echo "  $NUM_WORKERS workers ($PREEMPT_WORKERS pre-emptible)"
    echo "    Each has $CORES_PER_WORKER cores"
    echo "    Each has $RAM_PER_WORKER MB RAM"
    echo "    Each has $DISK_PER_WORKER GB Persistent disk"
    printf "    Cost will be $%0.2f/hour" "$COST"
    echo
}

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 number-of-cores"
    exit 1
fi

NUM_CORES=$1

if [[ "$NUM_CORES" -lt 32 ]]; then
    echo "Not really a cluster without at least 32 cores"
    exit 1
fi

echo "Solving for lowest price..."
LOWEST_PRICE=9999999
BEST_PRICE=""
HIGHEST_VALUE=9999999
BEST_VALUE=""
for CORES_PER_WORKER in $(seq 2 2 64);
do
    config

    CENTS=$(bc -l <<< "$COST * 100")
    CENTS=$(printf "%0.f" "$CENTS")
    # We lose ~1 core per worker to Spark/YARN/Hadoop/...
    EXECUTORS=$(bc -l <<< "$NUM_WORKERS * ($CORES_PER_WORKER - 1)" )
    PER_EXECUTOR=$(bc -l <<< "100 * $CENTS / $EXECUTORS" )
    PER_EXECUTOR=$(printf "%0.f" "$PER_EXECUTOR")

    if [[ "$PER_EXECUTOR" -le "$HIGHEST_VALUE" ]]; then
        HIGHEST_VALUE=$PER_EXECUTOR
        BEST_VALUE=$CORES_PER_WORKER
    fi

    if [[ "$CENTS" -le "$LOWEST_PRICE" ]]; then
        LOWEST_PRICE=$CENTS
        BEST_PRICE=$CORES_PER_WORKER
    fi

    PER_CENT=$(bc -l <<< "$PER_EXECUTOR / 10000 " )
    printf "    $%0.4f per executor for %s executors\n" "$PER_CENT" "$EXECUTORS"
    echo

done

echo
LOWEST_PRICE=$(bc -l <<< "$LOWEST_PRICE / 100")
printf "Best cost is : $%0.2f/hour at %d cores/worker\n" "$LOWEST_PRICE" "$BEST_PRICE"
HIGHEST_VALUE=$(bc -l <<< "$HIGHEST_VALUE / 10000")
printf "Best value is: $%0.4f/core hour at %d cores/worker\n" "$HIGHEST_VALUE" "$BEST_VALUE"

#CORES_PER_WORKER=$BEST_VALUE
#CORES_PER_WORKER=$BEST_PRICE
CORES_PER_WORKER=16
config

CMD="gcloud beta dataproc --region us-east4 \
    clusters create blast-dataproc-$USER-$(date +%Y%m%d-%-I) \
    --master-machine-type $MASTER \
        --master-boot-disk-size $DISK_PER_MASTER \
    --num-workers 2 \
        --worker-boot-disk-size $DISK_PER_WORKER \
    --worker-machine-type $WORKER \
    --num-preemptible-workers $PREEMPT_WORKERS \
        --preemptible-worker-boot-disk-size $DISK_PER_WORKER \
    --scopes cloud-platform \
    --project ncbi-sandbox-blast \
    --labels owner=$USER \
    --region us-east4 \
    --zone   us-east4-b \
    --max-age=8h \
    --image-version 1.2 \
    --properties dataproc:dataproc.monitoring.stackdriver.enabled=true,dataproc:dataproc.logging.stackdriver.enabled=true, \
    --initialization-action-timeout 30m \
    --initialization-actions \
    $PIPELINEBUCKET/scripts/cluster_initialize.sh,$PIPELINEBUCKET/scripts/ganglia.sh \
    --tags blast-dataproc-$USER-$(date +%Y%m%d-%H%M%S) \
    --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4"

echo "Command is: $CMD"
echo
EXECUTORS=$(bc -l <<< "$NUM_WORKERS * ($CORES_PER_WORKER - 1)" )
#echo "In ini.json set \"num_executors\" : $EXECUTORS ,"
echo
read -p "Press enter to start this cluster"

$CMD

exit 0


# dataproc-3bd9289a... has 15 day deletion lifecycle

# TODO: Scopes: storage-rw, pubsub, bigtable.admin.table, bigtabl.data,
# devstorage.full_control,
# gcloud dataproc clusters diagnose cluster-name
#--single-node

# Sizing and Pricing Notes
# ------------------------
# Larger nodes have faster network, and faster startup
# Takes an n1-standard-32 about 11 minutes to copy 50GB NT
# in parallel. No improvement seen by tar'ing them all up into one object to
# download.
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

# standard-16 and highcpu-64 have >58GB
# Likely need ~2 GB/vCPU: custom-8-16384?

# ~$15/hour for 56 node cluster
# ~$21/hour for 16 X 64
#    --worker-machine-type custom-64-71680 \

