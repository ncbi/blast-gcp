#!/bin/bash
#set -euxo pipefail
#set -o errexit
#set -o nounset
#set -o xtrace
#exec >  >(tee -ia /tmp/cluster_initialize.log)
#exec 2> >(tee -ia /tmp/cluster_initialize.log >&2)

# Copy this script to GS bucket with:
# gsutil cp  cluster_initialize.sh "$PIPELINEBUCKET/scripts/cluster_initialize.sh"

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

apt-get install libdw-dev nmap netcat -y

# /mnt/ram-disk
#phymem=$(free|awk '/^Mem:/{print $2}')
sudo mkdir /mnt/ram-disk
echo 'tmpfs /mnt/ram-disk tmpfs nodev,nosuid,noexec,nodiratime,size=50% 0 0' \
    | sudo tee -a /etc/fstab
sudo mount -t tmpfs -o size=50% /mnt/ram-disk


PIPELINEBUCKET="gs://blastgcp-pipeline-test"
DBBUCKET="gs://nt_50mb_chunks/"
BLASTTMP=/tmp/blast/
BLASTDBDIR=$BLASTTMP/db

mkdir -p $BLASTDBDIR

if [[ "${ROLE}" == 'Master' ]]; then
    # For master node only, skip copy
    echo "master node, skipping DB copy"
    # Auto terminate cluster in 8 hours
    sudo shutdown -h +480
else
    # Worker node, copy DBs from GCS
    # FIX: Future mapper will compute db lengths needed by Blast libraries
    MAXJOBS=8
    parts=`gsutil ls $DBBUCKET  | cut -d'.' -f2 | sort -Ru`
    for part in $parts; do
        piece="nt_50M.$part"
        mkdir -p $BLASTDBDIR/$piece
        cd $BLASTDBDIR/$piece
        #mkdir lock
        gsutil -m cp $DBBUCKET$piece.*in . &
        gsutil -m cp $DBBUCKET$piece.*sq . &
        gsutil -m cp $DBBUCKET$piece.*hr . &
        touch done
        #rmdir lock

        j=`jobs | wc -l`
        while [ $j -ge $MAXJOBS ]; do
            j=`jobs | wc -l`
            echo "$j waiting ..."
            sleep 0.5
        done
    done
fi

# Set lax permissions
cd $BLASTTMP
chown -R spark:spark $BLASTTMP
chmod -R ugo+rxw $BLASTTMP

ls -laR $BLASTTMP

echo Cluster Initialized
date

exit 0


# Future enhancements:
# run-init-actions-early? To get RAM before Spark/YARN?
# Cheap Chaos Monkey (shutdown -h +$RANDOM)
# Start daemons
# pre-warm databases
# Schedule things (cron or systemd timer)
# Configure user environments
# Submit stream, keep it alive:
#     https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/post-init

