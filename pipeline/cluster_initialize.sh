#!/bin/bash

apt-get install asn1c python-pyasn1 dumpasn1 libtasn1-bin maven libdw-dev -y

exit 0

# Copy to bucket with:
PIPELINEBUCKET="gs://blastgcp-pipeline-test"
gsutil cp  cluster_initialize.sh "$PIPELINEBUCKET/scipts/cluster_initialize.sh"



# TODOS:
# Separate master/worker configuration
# Auto terminate cluster (shutdown -h +14400)
# Cheap Chaos Monkey (shutdown -h +$RANDOM)
# Start daemons
# pre-warm databases
# Schedule things (cron or systemd timer)
# Configure user environments
# Submit stream, keep it alive:
#     https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/post-init


# Below is historical


set -euxo pipefail


set -o errexit
set -o nounset
set -o xtrace

exec >  >(tee -ia /tmp/cluster_initialize.log)
exec 2> >(tee -ia /tmp/cluster_initialize.log >&2)
echo Initializing Cluster
date

cd /tmp
# Need libdw for Blast library, maven for building

# Autokill cluster in 24 hours
shutdown -h +1440

<<"SKIP"
    # Set lax permissions for /tmp/blast
    mkdir -p /tmp/blast/db
    cd /tmp/blast/
    chown -R spark:spark /tmp/blast/
    chmod -R ugo+rw /tmp/blast

    # Copy stuff from GCS
    gsutil -m cp gs://blastgcp-pipeline-test/dbs/nt04.tar .
    cd /tmp/blast/db
    tar -xvf ../nt04.tar
    rm -f ../nt04.tar
    cd /tmp/blast
    #gsutil cp gs://blastgcp-pipeline-test/libs/* .
    #chmod ugo+rx *.so
    chown -R spark:spark /tmp/blast/
    chmod -R ugo+rw /tmp/blast


    # For master node only below this:
    ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
    if [[ "${ROLE}" == 'Master' ]]; then
        echo "master only now"

        # Need maven for building
        apt-get install maven -y
    fi
SKIP

echo Cluster Initialized
date

exit 0
