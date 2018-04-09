#!/bin/bash
#set -euo pipefail

apt-get install asn1c python-pyasn1 dumpasn1 libtasn1-bin maven libdw-dev -y

BLASTTMP=/tmp/blast/
BLASTDBDIR=$BLASTTMP/db

# Copy stuff from GCS
mkdir -p $BLASTDBDIR/nt.04
cd $BLASTDBDIR/nt.04
gsutil -m cp gs://blastgcp-pipeline-test/dbs/nt04.tar .
tar -xvf nt04.tar
rm -f nt04.tar

#mkdir -p $BLASTDBDIR/all
#cd $BLASTDBDIR/all
#gsutil -m cp gs://blastgcp-pipeline-test/dbs/nt_all.tar .
#tar -xvf nt_all.tar
#rm -f nt_all.tar

MAXJOBS=8
parts=`gsutil ls gs://nt_50mb_chunks/  | cut -d'.' -f2 | sort -nu`
for part in $parts; do
    piece="nt_50M.$part"
    mkdir -p $BLASTDBDIR/$piece
    cd $BLASTDBDIR/$piece
    #mkdir lock
    gsutil -m cp gs://nt_50mb_chunks/$piece.*in . &
    gsutil -m cp gs://nt_50mb_chunks/$piece.*sq . &
    touch done
    #rmdir lock

    j=`jobs | wc -l`
    while [ $j -ge $MAXJOBS ]; do
        j=`jobs | wc -l`
        echo "$j waiting ..."
        sleep 0.5
    done
done


# Set lax permissions
cd $BLASTTMP
chown -R spark:spark $BLASTTMP
chmod -R ugo+rxw $BLASTTMP

ls -laR $BLASTTMP

echo Cluster Initialized
date
exit 0

# Copy to bucket with:
PIPELINEBUCKET="gs://blastgcp-pipeline-test"
#gsutil cp  cluster_initialize.sh "$PIPELINEBUCKET/scipts/cluster_initialize.sh"



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

    # For master node only below this:
    ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
    if [[ "${ROLE}" == 'Master' ]]; then
        echo "master only now"

        # Need maven for building
        apt-get install maven -y
    fi
SKIP

exit 0
