#!/bin/bash

set -o errexit
set -o nounset
set -o xtrace

exec >  >(tee -ia /tmp/cluster_initialize.log)
exec 2> >(tee -ia /tmp/cluster_initialize.log >&2)
date
pwd >> /tmp/cluster_initialize.log

cd /tmp
# Need libdw for Blast library
apt-get install libdw-dev -y

# Autokill cluster in 24 hours
shutdown -h +1440

# Copy stuff from GCS
mkdir -p /tmp/blast/db
chown -R spark:spark /tmp/blast/
chmod -R ugo+rw /tmp/blast

cd /tmp/blast/
gsutil -m cp gs://blastgcp-pipeline-test/dbs/nt04.tar .
cd /tmp/blast/db
tar -xvf ../nt04.tar
rm -f ../nt04.tar
cd /tmp/blast
gsutil cp gs://blastgcp-pipeline-test/libs/* .
chmod ugo+rx *.so
chown -R spark:spark /tmp/blast/
chmod -R ugo+rw /tmp/blast



# For master node only below this:
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
    echo "master"
#  gsutil cp gs://my-bucket/jobs/sessionalize-logs-1.0.jar home/username
fi

[[ "${HOSTNAME}" =~ -m$ ]] || exit 0
# Need maven for building
apt-get install maven -y
exit 0
