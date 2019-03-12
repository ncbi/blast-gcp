#!/bin/bash
set -o nounset # same as -u
set -o errexit # same as -e
#set -o pipefail
#shopt -s nullglob globstar #

BC_CLASS="gov.nih.nlm.ncbi.blastjni.BC_MAIN"
BC_JAR="./target/sparkblast-1-jar-with-dependencies.jar"
BC_INI="ini.json"

which asntool || sudo apt install ncbi-tools-bin

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .

[ -d report ] && rm -rf report/
[ -d stability_test ] || mkdir -p stability_test

echo "Downloading test queries..."
gsutil -m cp -n "gs://blast-test-requests-sprint11/*.json"  \
	stability_test/ > /dev/null 2>&1
echo "Downloaded test queries."

find stability_test -name "*json" | \
	head -10 > stability_test/stability_tests.txt

echo "L stability_test/stability_tests.txt" | \
	spark-submit --master yarn --class $BC_CLASS $BC_JAR $BC_INI


cd report
grep -h "done at" ./*.txt | sort > dones
for asn in *.asn1; do
	asntool -m ../../lib_builder/asn.all -t Seq-annot -d "$asn" -p "$asn.txt"
done

rm -f ./*.result

md5sum -- *.asn1* | sort > asn1.md5sum.result
md5sum -- *.asn1.txt* | sort > asn1.txt.md5sum.result

diff ../asn1.md5sum.expected asn1.md5sum.result
if [ "$?" -ne 0 ]; then
	echo "Differences in .asn1 output"
fi
diff ../asn1.txt.md5sum.expected asn1.txt.md5sum.result
if [ "$?" -ne 0 ]; then
	echo "Differences in .asn1.txt output"
fi
