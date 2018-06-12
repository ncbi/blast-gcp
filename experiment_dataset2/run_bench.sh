#!/bin/bash

MAXJOBS=64

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"

if [ ! -e nt_50M.613.nsi ]; then
    gsutil -m cp gs://nr_50mb_chunks/*.613.* .
    gsutil -m cp gs://nt_50mb_chunks/*.613.* .
fi

SPARK_TEST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_BENCH"
SPARK_TEST_JAR="./target/sparkblast-1-jar-with-dependencies.jar"

for test in A*json; do
    echo -n "Checking $test..."
    ~/blast-gcp/experiment_dataset2/fixjson.py $test
    java -Xcheck:jni -Xdiag -Xfuture \
        -Djava.library.path="." \
        -cp $SPARK_TEST_JAR:.:/usr/local/spark/2.2.0/jars/* \
        $SPARK_TEST_CLASS \
        "$test.fix" > "$test.result" 2>&1 &
    j=`jobs | wc -l`
    while [ $j -ge $MAXJOBS ]; do
        j=`jobs | wc -l`
    done

done
