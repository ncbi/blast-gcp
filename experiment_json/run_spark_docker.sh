#!/bin/bash

set -e

usage="
usage: $0
Required environment variables:
    project
    region
    deploy_id
    deploy_user
    result_bucket_name
    joborch_output_subscrip
    blast_dataproc_cluster_name
Optional environment variables:
    NUM_EXECUTORS (defaults to 126)
"

[ $# -eq 0 ] || { echo ${usage} && exit 1; }

checkvar=${project?"${usage}"}
checkvar=${region?"${usage}"}
checkvar=${deploy_id?"${usage}"}
checkvar=${deploy_user?"${usage}"}
checkvar=${result_bucket_name?"${usage}"}
checkvar=${joborch_output_subscrip?"${usage}"}
checkvar=${blast_dataproc_cluster_name?"${usage}"}

NUM_EXECUTORS=${NUM_EXECUTORS-126}

SPARK_BLAST_JAR="sparkblast.jar"
SPARK_BLAST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_MAIN"

INI=$(cat <<-END
{
    "appName": "blast_pipeline",
    "source":
    {
        "pubsub":
        {
            "project_id": "PROJECT",
            "subscript_id": "PUBSUB",
            "use": "true"
        },
    },

    "blastjni":
    {
        "num_db_limit": 1089,
        "manifest_root": "dbs_50M.json"
    },

    "result":
    {
        "asn1":
        {
            "bucket": "RESULTBUCKET"
        },
        "status":
        {
            "bucket": "RESULTBUCKET"
        }
    },

    "spark":
    {
        "num_executors": NUM_EXECUTORS,
        "num_executor_cores": 1
    }
}
END
)

INI=${INI//PROJECT/$project}
INI=${INI//PUBSUB/$joborch_output_subscrip}
INI=${INI//RESULTBUCKET/$result_bucket_name}
INI=${INI//NUM_EXECUTORS/$NUM_EXECUTORS}

INI_JSON=ini_docker.json
printf "$INI\n" > ${INI_JSON}

gcloud dataproc jobs submit spark --project ${project} --region ${region} --cluster "${blast_dataproc_cluster_name}" \
    --labels owner=${deploy_user},deploy_id=${deploy_id} \
    --jars ${SPARK_BLAST_JAR} --class ${SPARK_BLAST_CLASS} \
    --files dbs.json,libblastjni.so,blast_json,${INI_JSON} -- ${INI_JSON}

