#!/bin/bash

set -e

usage="
usage: $0
Required environment variables:
    project
    result_bucket_name
    joborch_output_topic
    blast_dataproc_cluster_name
Optional environment variables:
    NUM_EXECUTORS (defaults to 126)
"

[ $# -eq 0 ] || { echo ${usage} && exit 1; }

checkvar=${project?"${usage}"}
checkvar=${result_bucket_name?"${usage}"}
checkvar=${joborch_output_topic?"${usage}"}
checkvar=${blast_dataproc_cluster_name?"${usage}"}

NUM_EXECUTORS=${NUM_EXECUTORS-126}

SPARK_BLAST_JAR="sparkblast-1-jar-with-dependencies.jar"
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
                "extensions" : [ "nhr", "nin", "nsq" ],
                "num_partitions" : 887,
                "num_locations" : 2
            },
            {
                "selector" : "nr",
                "bucket"   : "nr_50mb_chunks",
                "pattern"  : "nr_50M",
                "extensions" : [ "phr", "pin", "psq" ],
                "num_partitions" : 1086,
                "num_locations" : 1
            }
        ],
        "jni_log_level": "DEBUG",
        "top_n" : 100
    },

    "result" :
    {
        "asn1" :
        {
            "bucket" : "RESULTBUCKET"
        },
        "status" :
        {
            "bucket"  : "RESULTBUCKET"
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
        "locality_wait" : "30s"
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
INI=${INI//PUBSUB/$joborch_output_topic}
INI=${INI//RESULTBUCKET/$result_bucket_name}
INI=${INI//NUM_EXECUTORS/$NUM_EXECUTORS}

printf "$INI\n" > /app/ini_docker.json

gcloud dataproc jobs submit spark --project ${project} --cluster "${blast_dataproc_cluster_name}" \
    --jars ${SPARK_BLAST_JAR} --class ${SPARK_BLAST_CLASS} \
    -- /app/ini_docker.json

