#!/bin/bash

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"

if [ -e nt_50M.14.psi ]; then
    gsutil cp gs://nr_50mb_chunks/*.14.* .
    gsutil cp gs://nt_50mb_chunks/*.14.* .
fi

SPARK_TEST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_TEST"
SPARK_TEST_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
MAXJOBS=15

pushd tests
#gsutil -m cp gs://blast-test-requests-sprint6/*json .
rm -f *.json.out *.json.fix
#tar -xf orig.tar
popd

for test in tests/A*json; do
    OUT=$test.out
    ./fixjson.py $test
    echo "Running $test"
    echo "$test" >> $OUT
    nice java -Xcheck:jni -Xfuture \
        -Djava.library.path="." \
        -cp $SPARK_TEST_JAR:.:/usr/local/spark/2.2.0/jars/* \
        $SPARK_TEST_CLASS \
        $test.fix >> $OUT 2>&1 &
    j=`jobs | wc -l`
    while [ $j -ge $MAXJOBS ]; do
        j=`jobs | wc -l`
    done

done
