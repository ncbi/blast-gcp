#!/bin/bash
set -o nounset # same as -u
set -o errexit # same as -e
#set -o pipefail
#shopt -s nullglob globstar #

BC_CLASS="gov.nih.nlm.ncbi.blastjni.BC_MAIN"
BC_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
BC_INI="ini_test.json"

command -v asntool || sudo apt install -y ncbi-tools-bin

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .

[ -d report ] && rm -rf report/
[ -d stability_test ] || mkdir -p stability_test

echo "Downloading test queries..."
gsutil -m cp -n "gs://blast-test-requests-sprint11/*.json"  \
    stability_test/ > /dev/null 2>&1
echo "Downloaded test queries."

find stability_test -name "*json" \
    | sort -R \
    > stability_test/stability_tests.txt

cat << EOF > $BC_INI
    {
        "databases" :
        [
            {
                "key" : "nr",
                "worker_location" : "/tmp/blast/db",
                "source_location" : "gs://nr_50mb_chunks",
                "extensions" : [ "psq", "pin", "phr" ],
                "limit" : 1086
            },
            {
                "key" : "nt",
                "worker_location" : "/tmp/blast/db",
                "source_location" : "gs://nt_50mb_chunks",
                "extensions" : [ "nsq", "nin", "nhr" ],
                "limit" : 887
            }
        ],
        "cluster" :
        {
            "transfer_files" : [ "libblastjni.so" ],
            "num_executors" : 256,
            "num_executor_cores" : 2,
            "parallel_jobs" : 20,
            "jni_log_level" : "WARN"
        }

    }
EOF

echo -e "I\nL stability_test/stability_tests.txt\nI\n" | \
    spark-submit --master yarn --class $BC_CLASS $BC_JAR $BC_INI

cd report
grep -h "done at" ./*.txt | sort > dones
for asn in *.asn1; do
    asntool -m ../../lib_builder/asn.all -t Seq-annot -d "$asn" -p "$asn.txt"
done

rm -f ./*.result

wc -l -- *.asn1 | sort > asn1.wc.result
wc -l -- *.asn1.txt | sort > asn1.txt.wc.result

if diff ../asn1.wc.expected asn1.wc.result; then
#if [ "$?" -ne 0 ]; then
    echo "Differences in .asn1 output"
fi

if diff ../asn1.txt.wc.expected asn1.txt.wc.result; then
    echo "Differences in .asn1.txt output"
fi
