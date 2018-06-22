#!/bin/bash

MAXJOBS=40

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"

cd $TMP

if [ ! -e nt_50M.613.nsi ]; then
    gsutil -m cp gs://nr_50mb_chunks/*.613.* .
    gsutil -m cp gs://nt_50mb_chunks/*.613.* .
fi

SPARK_TEST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_TEST"
SPARK_TEST_JAR="/home/vartanianmh/blast-gcp/experiment_dataset2/target/sparkblast-1-jar-with-dependencies.jar"
cp ~/blast-gcp/experiment_dataset2/libblastjni.so .

mkdir -p tests
pushd tests
    rm -f *.json.out *.json.fix $TMP/*.out
    gsutil -m cp -n gs://blast-test-requests-sprint6/*json .
popd

for test in tests/A*json; do
    test="$test.json"
    ~/blast-gcp/experiment_dataset2/fixjson.py $test
    OUT="$TMP/$(basename $test).out"
    echo $OUT
    echo "Running $test -> $OUT"
    echo "$test" >> $OUT
    nice java -Xcheck:jni -Xfuture \
        -Xmx1g \
        -XX:+UseSerialGC \
        -Djava.library.path=".:/home/vartanianmh/blast-gcp/experiment_dataset2" \
        -cp $SPARK_TEST_JAR:.:/home/vartanianmh/blast-gcp/experiment_dataset2:/usr/local/spark/2.2.0/jars/* \
        $SPARK_TEST_CLASS \
        $test.fix >> $OUT 2>&1 &
    j=`jobs | wc -l`
    while [ $j -ge $MAXJOBS ]; do
        j=`jobs | wc -l`
    done
done
