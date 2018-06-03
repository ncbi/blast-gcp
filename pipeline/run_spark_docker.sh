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
    "appName" : "blast_pipeline",
    "source" :
    {
        "pubsub" :
        {
            "project_id" : "PROJECT",
            "subscript_id" : "PUBSUB",
            "use" : "true"
        },
        "socket" :
        {
            "trigger_port" : 10012,
            "use" : "false"
        },
        "hdfs" :
        {
            "dir" : "hdfs:///user/%s/requests",
            "use" : "false"
        }
    },

    "blastjni" :
    {
        "db" :
        [
            {
                "selector" : "nt",
                "bucket"   : "nt_50mb_chunks",
                "pattern"  : "nt_50M",
                "extensions" : [ "nax", "nin", "nsq" ],
                "num_partitions" : 887,
                "num_locations" : 2
            },
            {
                "selector" : "nr",
                "bucket"   : "nr_50mb_chunks",
                "pattern"  : "nr_50M",
                "extensions" : [ "pax", "pin", "psq" ],
                "num_partitions" : 1086,
                "num_locations" : 1
            }
        ],
        "jni_log_level": "DEBUG",
        "top_n" : 100,
        "num_db_limit" : 160,
        "num_locations" : 2,
        "manifest_root" : "dbs.json"
    },

    "result" :
    {
        "asn1" :
        {
            "bucket": "RESULTBUCKET",
            "file": "%s/seq-annot.asn",
            "gs_or_hdfs": "gs"
        },
        "status" :
        {
            "bucket"  : "RESULTBUCKET",
            "file" : "%s/status.txt"
        }
    },

    "spark" :
    {
        "transfer_files" : [ "libblastjni.so" ],
        "shuffle_reduceLocality_enabled" : "false",
        "scheduler_fair" : "false",
        "with_locality" : "true",
        "num_executors" : NUM_EXECUTORS,
        "num_executor_cores" : 1,
        "executor_memory" : "1G",
        "locality_wait" : "30s",
        "parallel_jobs" : 1
    },

    "log" :
    {
        "log_port" : 10011,
        "pref_loc" : "false",
        "part_prep" : "true"
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
    --files dbs.json,libblastjni.so,${INI_JSON} -- ${INI_JSON}

