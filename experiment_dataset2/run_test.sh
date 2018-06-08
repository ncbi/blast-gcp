#!/bin/bash

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"

if [ -e nt_50M.14.psi ]; then
    gsutil cp gs://nr_50mb_chunks/*.14.* .
    gsutil cp gs://nt_50mb_chunks/*.14.* .
fi

SPARK_TEST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_TEST"
SPARK_TEST_JAR="./target/sparkblast-1-jar-with-dependencies.jar"

for test in AFE*json; do
    echo "Running $test"
    java -Xcheck:jni -Xdiag -Xfuture \
        -Djava.library.path="." \
        -cp $SPARK_TEST_JAR:.:/usr/local/spark/2.2.0/jars/* \
        $SPARK_TEST_CLASS \
        $test > $test.out 2>&1
done
