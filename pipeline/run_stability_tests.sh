#!/bin/bash
set -o nounset # same as -u
set -o errexit # same as -e

unset LC_ALL # Messes with sorting

BC_CLASS="gov.nih.nlm.ncbi.blastjni.BC_MAIN"
BC_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
BC_INI="ini_test.json"
LOG_CONF="--driver-java-options=-Dlog4j.configuration=file:log4j.properties"

command -v asntool > /dev/null || sudo apt install -y ncbi-tools-bin

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .

[ -d report ] && rm -rf report/
[ -d stability_test ] || mkdir -p stability_test

echo "Downloading test queries..."
gsutil -m cp -n "gs://blast-test-requests-sprint11/*.json"  \
    stability_test/ > /dev/null 2>&1

numtests=$(find stability_test/ -name "*.json" | wc -l)
echo "Downloaded $numtests test queries."

# Query databases in order
find stability_test/ -name "*.json" | sort > stability_test/stability_tests.txt

cat << EOF > $BC_INI
    {
        "databases" :
        [
            {
                "key" : "nr",
                "worker_location" : "/tmp/blast/db",
                "source_location" : "gs://nr_50mb_chunks",
                "extensions" : [ "psq", "pin", "pax" ]
            },
            {
                "key" : "nt",
                "worker_location" : "/tmp/blast/db",
                "source_location" : "gs://nt_50mb_chunks",
                "extensions" : [ "nsq", "nin", "nax" ]
            }
        ],
        "cluster" :
        {
            "transfer_files" : [ "libblastjni.so" ],
            "parallel_jobs" : 48,
            "num-executor-cores": 1,
            "log_level" : "INFO",
            "jni_log_level" : "INFO"
        }
    }
EOF

echo -e ":wait\n:exit\n" \
 >> stability_test/stability_tests.txt

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .
spark-submit --master yarn \
    "$LOG_CONF" \
    --class $BC_CLASS $BC_JAR $BC_INI stability_test/stability_tests.txt


cd report || exit
grep -h "done at" ./*.txt | sort > dones
for asn in *.asn1; do
    asntool -m ../../lib_builder/asn.all \
        -t Seq-annot -d "$asn" -p "$asn.txt"
done

unset LC_ALL # Messes with sorting
wc -l ./*.asn1.txt | awk '{print $1 "\t" $2;}' | sort -k2 > ../asn1.txt.wc.result

DATE=$(date "+%Y%m%d%H%M")
gsutil -m cp -r ./* "gs://blast-stability-test-results/$DATE"

cd ..
if diff asn1.txt.wc.expected asn1.txt.wc.result; then
    echo "Differences in .asn1.txt output"
fi

echo "Downloading reference results..."
gsutil -m cp -nr gs://blast-results-reference/ . > /dev/null 2>&1
numrefs=$(find blast-results-reference/ -name "*.gz" | wc -l)
echo "Downloaded $numrefs reference results."

echo "Uncompressing reference results..."
cd blast-results-reference || exit
find ./ -name "*gz" -print0 | nice xargs -0 -n 8 -P 8 gunzip
echo "Uncompressed  reference results."
cd ..

for check in blast-results-reference/*.asn; do
    base=$(basename "$check")
    req="report/REQ_${base}1.txt"
    diff "$check" "$req" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "Differences with $check $req"
    else
        echo "Ok with $base"
    fi
done
