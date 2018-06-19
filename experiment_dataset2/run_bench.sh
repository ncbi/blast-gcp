#!/bin/bash

MAXJOBS=8

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"

cd $TMP
gsutil -m cp -n gs://nr_50mb_chunks/*pax .
gsutil -m cp -n gs://nr_50mb_chunks/*pin .
gsutil -m cp -n gs://nr_50mb_chunks/*psq .
gsutil -m cp -n gs://nt_50mb_chunks/*nax .
gsutil -m cp -n gs://nt_50mb_chunks/*nin .
gsutil -m cp -n gs://nt_50mb_chunks/*nsq .

SPARK_TEST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_BENCH"
SPARK_TEST_JAR="/home/vartanianmh/blast-gcp/experiment_dataset2/target/sparkblast-1-jar-with-dependencies.jar"

for test in tests/*json; do
    echo -n "Checking $test..."
    ~/blast-gcp/experiment_dataset2/fixjson.py $test
    nice java -Xdiag -Xfuture \
        -Xmx2g \
        -XX:+UseSerialGC \
        -Djava.library.path="." \
        -cp $SPARK_TEST_JAR:.:/usr/local/spark/2.2.0/jars/* \
        $SPARK_TEST_CLASS \
        "$test.fix" > "$test.result" 2>&1 &
    j=`jobs | wc -l`
    while [ $j -ge $MAXJOBS ]; do
        j=`jobs | wc -l`
    done

done
