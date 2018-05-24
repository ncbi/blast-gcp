#!/bin/bash

# Args are:
# 1 project
# 2 pubsub subscription
# 3 asn bucket
# 4 status bucket
# 5 cluster

PROJECT=$1
PUBSUB=$2
ASNBUCKET=$3
RESULTBUCKET=$4
CLUSTER=$5

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
                "bucket" : "ASNBUCKET"
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

INI=${INI//PROJECT/$PROJECT}
INI=${INI//PUBSUB/$PUBSUB}
INI=${INI//ASNBUCKET/$ASNBUCKET}
INI=${INI//RESULTBUCKET/$RESULTBUCKET}

echo "$INI" >> ini_docker.json

echo gcloud dataproc jobs submit spark \
    --cluster "$CLUSTER" \
    --jar sparkblast-1-jar-with-dependencies.jar \
    ini_docker.json
