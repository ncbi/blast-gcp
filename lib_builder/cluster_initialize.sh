#!/bin/bash

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

# Copy this script to GS bucket with:
# gsutil cp  cluster_initialize.sh "$PIPELINEBUCKET/scripts/cluster_initialize.sh"

cd /tmp

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

BLASTTMP=/tmp/blast/
BLASTDBDIR=$BLASTTMP/db/

curl -sSO https://dl.google.com/cloudagents/install-monitoring-agent.sh
sudo bash install-monitoring-agent.sh | tee -a stackdriver-install.log 2>&1

curl -sSO https://dl.google.com/cloudagents/install-logging-agent.sh
sudo bash install-logging-agent.sh --structured | tee -a stackdriver-install.log 2>&1

cd /tmp
cat << DONE > libblast-log.conf
<source>
# Automatically generated by cluster_initialize.sh
    @type tail
    format syslog
    path /tmp/blastjni.*.log
    pos_file /var/tmp/fluentd.blastjni.pos
    read_from_head true
    tag blastjni-log
</source>
DONE
cp libblast-log.conf /etc/google-fluentd/config.d/libblast-log.conf
service google-fluentd restart


cat << 'DONE2' > log4j.proto
# Automatically generated by cluster_initialize.sh

log4j.appender.tmpfile=org.apache.log4j.FileAppender
log4j.appender.tmpfile.File=/tmp/blastjni.${user.name}.log
log4j.appender.tmpfile.layout=org.apache.log4j.PatternLayout
log4j.appender.tmpfile.layout.ConversionPattern=%m%n

log4j.appender.sparkfile=org.apache.log4j.FileAppender
log4j.appender.sparkfile.File=/var/log/spark/blastjni.${user.name}.log
log4j.appender.sparkfile.layout=org.apache.log4j.PatternLayout
log4j.appender.sparkfile.layout.ConversionPattern=%d [%p] [%t] %c: %m%n

# Spark/JNI layers will further restrict on a per query basis
log4j.logger.gov.nih.nlm.ncbi.blastjni=DEBUG, tmpfile, sparkfile
log4j.logger.gov.nih.nlm.ncbi.blastjni.BLAST_BENCH=DEBUG, tmpfile
log4j.logger.gov.nih.nlm.ncbi.blastjni.BLAST_TEST=INFO, tmpfile

DONE2
cat log4j.proto >> /etc/spark/conf.dist/log4j.properties


logger -t cluster_initialize.sh "BLASTJNI Logging agent begun with cluster_initialize.sh"

# Auto terminate cluster in 8 hours, now handled by max-age in make_cluster.sh
# sudo shutdown -h +480

mkdir -p $BLASTDBDIR

if [[ "${ROLE}" == 'Master' ]]; then
    echo "master node"
    # Need maven to build jars, virtualenv for installing Google APIs for tests
    apt-get update -y
    apt-get install -y -u maven python python-dev python3 python3-dev virtualenv
    # protobuf-compiler
    # chromium, xterm # for looking at webserver with X11 forwarding?
else
    echo "worker node"
fi

# Set lax permissions
cd $BLASTTMP
chown -R spark:spark $BLASTTMP
chmod -R ugo+rxw $BLASTTMP

ls -laR $BLASTTMP

echo Cluster Initialized
logger -t cluster_initialize.sh "BLASTJNI cluster_initialize.sh complete"
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

