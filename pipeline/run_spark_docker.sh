#!/bin/bash

# Args are:
# 1 asn bucket
# 2 cluster

set -e

usage="
usage: $0 <cluster>
Required environment variables:
    project
    region
    result_bucket_name
    joborch_output_topic
"

[ $# -eq 1 ] || { echo ${usage} && exit 1; }

CLUSTER=$1

checkvar=${project?"${usage}"}
checkvar=${region?"${usage}"}
checkvar=${result_bucket_name?"${usage}"}
checkvar=${joborch_output_topic?"${usage}"}


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
        "num_executors" : 400,
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

printf "$INI\n" > /app/ini_docker.json

gcloud dataproc jobs submit spark --project ${project} --region ${region} --cluster "${CLUSTER}" \
    --jar sparkblast-1-jar-with-dependencies.jar \
    -- /app/ini_docker.json

