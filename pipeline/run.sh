#!/bin/bash

BC_CLASS="gov.nih.nlm.ncbi.blastjni.BC_MAIN"
BC_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
BC_INI="ini.json"
LOG_CONF="--driver-java-options=-Dlog4j.configuration=file:log4j.properties"

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .

rm -rf report/*

spark-submit --master yarn $LOG_CONF --class $BC_CLASS $BC_JAR $BC_INI $1

