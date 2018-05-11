#!/bin/bash

echo "preparing a worker-node"

sudo apt-get install libdw-dev -y

mkdir -p /tmp/blast/db
cd /tmp/blast/    
gsutil -m cp gs://blastgcp-pipeline-test/dbs/nt04.tar .
cd /tmp/blast/db
tar -xvf ../nt04.tar
cd /tmp/blast
rm -rf /tmp/blast/nt04.tar
sudo chown -R spark:spark /tmp/blast/
sudo chmod -R ugo+rw /tmp/blast
