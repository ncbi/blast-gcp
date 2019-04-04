#!/bin/bash
# delete-downloaded-dbs.sh: Delete all BLASTDBs downloaded by workers
# N.B.: This script is meant to run on a GCP dataproc master node

REGION=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/zone" -H "Metadata-Flavor: Google" | cut -d / -f 4 | sed 's/-.$//')
CLUSTER_NAME=$(hostname | sed 's/-m$//')
NUM_WORKERS=$(gcloud dataproc clusters describe $CLUSTER_NAME --region $REGION | grep numInstances | tail -1 | cut -d : -f 2 | tr -d ' ')
BASENAME=$CLUSTER_NAME-w-

echo "worker number-of-db-chunks"
for n in $(seq 0 $(( $NUM_WORKERS - 1 )) ) ; do
    num_chunks=`ssh $BASENAME$n ls -1 /tmp/blast/db/| wc -l`
    echo $BASENAME$n $num_chunks
done
