#!/bin/bash

BC_CLASS="gov.nih.nlm.ncbi.blastjni.BC_MAIN"
BC_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
BC_INI="ini.json"

spark-submit --master yarn --class $BC_CLASS $BC_JAR $BC_INI
