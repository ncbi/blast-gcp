#!/bin/bash

if [ $# -eq 0 ] ; then
    echo "Usage: $0 <input-file> [nt|nr]"
    echo -e "\tif the input file name has blastn or blastp in its name, the database will be set appropriately"
    exit 0
fi

DB=${2:-""}
if [[ "$1" =~ .*blastn.* ]] || [[ "$1" =~ .*nt.* ]] ; then
    DB=nt
else
    DB=nr
fi


BC_CLASS="gov.nih.nlm.ncbi.blastjni.BC_MAIN"
BC_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
BC_INI="$DB-ini.json"
LOG_CONF="--driver-java-options=-Dlog4j.configuration=file:log4j.properties"

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .

rm -rf report/*

# NOTE: Any changes below should also be in run_stability_tests.sh
spark-submit --master yarn $LOG_CONF --class $BC_CLASS $BC_JAR $BC_INI $1

