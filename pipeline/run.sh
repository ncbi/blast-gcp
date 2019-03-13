#!/bin/bash

BC_CLASS="gov.nih.nlm.ncbi.blastjni.BC_MAIN"
BC_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
BC_INI="ini.json"

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .

spark-submit --master yarn --class $BC_CLASS $BC_JAR $BC_INI $1

