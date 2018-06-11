#!/bin/bash

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"

if [ ! -e nt_50M.613.nsi ]; then
    gsutil -m cp gs://nr_50mb_chunks/*.613.* .
    gsutil -m cp gs://nt_50mb_chunks/*.613.* .
fi

SPARK_TEST_CLASS="gov.nih.nlm.ncbi.blastjni.BLAST_TEST"
SPARK_TEST_JAR="./target/sparkblast-1-jar-with-dependencies.jar"

for test in AFE*json; do
    echo -n "Checking $test..."
    java -Xcheck:jni -Xdiag -Xfuture \
        -Djava.library.path="." \
        -cp $SPARK_TEST_JAR:.:/usr/local/spark/2.2.0/jars/* \
        $SPARK_TEST_CLASS \
        "$test" 2>&1 | grep JSON > "$test.result"

    cmp "$test.result" "$test.expected"
    if [[ $? -ne 0 ]]; then
        echo "Difference in $test"
        sdiff -w 70 "$test.result" "$test.expected"
        exit 1
    else
        echo "OK"
    fi

done
