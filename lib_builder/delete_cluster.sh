#!/bin/bash -x
# delete_cluster.sh: terminates a cluster started via make_cluster_fixed_config.sh
set -euo pipefail
gcloud dataproc clusters delete blast-dataproc-$USER --region us-east4
