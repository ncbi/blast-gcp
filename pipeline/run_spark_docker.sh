#!/usr/bin/env bash
set -e

usage="
usage: $0 <ini.json>
Required environment variables:
    project
    region
    labels
    blast_dataproc_cluster_name
"

[ $# -eq 1 ] || { echo ${usage} && exit 1; }

INI_JSON=${1}
INI_JSON_FILENAME=${1##*/}

checkvar=${project?"${usage}"}
checkvar=${region?"${usage}"}
checkvar=${blast_dataproc_cluster_name?"${usage}"}
checkvar=${labels?"${usage}"}

SPARK_BLAST_JAR="sparkblast.jar"
SPARK_BLAST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_MAIN"

gcloud dataproc jobs submit spark --project ${project} --region ${region} --cluster "${blast_dataproc_cluster_name}" \
    --labels ${labels} \
    --jars ${SPARK_BLAST_JAR} --class ${SPARK_BLAST_CLASS} \
    --files /etc/blast/dbs.json,libblastjni.so,${INI_JSON} -- ${INI_JSON_FILENAME}

