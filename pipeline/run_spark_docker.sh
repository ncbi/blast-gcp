#!/usr/bin/env bash
set -e

usage="
usage: $0 <ini.json>
Required environment variables:
    project
    region
    deploy_id
    deploy_user
    blast_dataproc_cluster_name
"

[ $# -eq 1 ] || { echo ${usage} && exit 1; }

INI_JSON=${1}

checkvar=${project?"${usage}"}
checkvar=${region?"${usage}"}
checkvar=${deploy_id?"${usage}"}
checkvar=${deploy_user?"${usage}"}
checkvar=${blast_dataproc_cluster_name?"${usage}"}

SPARK_BLAST_JAR="sparkblast.jar"
SPARK_BLAST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_MAIN"

gcloud dataproc jobs submit spark --project ${project} --region ${region} --cluster "${blast_dataproc_cluster_name}" \
    --labels owner=${deploy_user},deploy_id=${deploy_id} \
    --jars ${SPARK_BLAST_JAR} --class ${SPARK_BLAST_CLASS} \
    --files dbs.json,libblastjni.so,${INI_JSON} -- ${INI_JSON}

