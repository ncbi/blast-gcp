#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

set +errexit
distro=$(grep Debian /etc/os-release | wc -l)
set -o errexit
if [ "$distro" -ne 0 ]; then
    echo "Building at Google on Debian 8"
    export ONGCP="true"
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
    export LD_LIBRARY_PATH="/tmp/blast:."
    export BLASTDB=/tmp/blast/db
#    JAVA_INC="-I$JAVA_HOME/include"
else
    echo "Building at NCBI on CentOS 7"
    export ONGCP="false"
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
    export LD_LIBRARY_PATH="/tmp/blast:.:/opt/ncbi/gcc/4.9.3/lib64/"
    export BLASTDB=/net/napme02/vol/blast/db/blast
fi

JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
export CLASSPATH="."

HDR="gov_nih_nlm_ncbi_blastjni_BlastJNI.h"
#rm -rf /tmp/blast
rm -f $HDR
rm -f BlastJNI.class
rm -f blastjni.o
rm -rf gov
rm -f *test.result
rm -rf target
rm -f BlastJNI.jar
rm -f db_partitions.json db_partitions.jsonl
#rm -f src.zip
#rm -f blast4spar*pp

#curl -o blast4spark.cpp https://svn.ncbi.nlm.nih.gov/viewvc/toolkit/branches/blast_gcp/src/algo/blast/api/blast4spark.cpp?view=co&content-type=text%2Fplain
#curl -o blast4spark.hpp https://svn.ncbi.nlm.nih.gov/viewvc/toolkit/branches/blast_gcp/include/algo/blast/api/blast4spark.hpp?view=co&content-type=text%2Fplain

echo "Compiling BlastJNI.java"
mkdir -p gov/nih/nlm/ncbi
javac -d gov/nih/nlm/ncbi -h . BlastJNI.java
#rm -rf gov
echo "/*" >> $HDR
echo "$USER" >> $HDR
javac -version >> $HDR 2>&1
g++ --version | head -1 >> $HDR
#date >> $HDR
echo "*/" >> $HDR
# javac
#javac -d gov/nih/nlm/ncbi BlastJNI.java
javac -d . BlastJNI.java
#cp BlastJNI.class gov/nih/nlm/ncbi

if [ "$ONGCP" = "false" ]; then
    echo "Compiling blastjni.cpp"
    rm -f blastjni.so
    g++ blastjni.cpp \
        -L./int/blast/libs \
        -std=gnu++11 \
        -Wall -g  -I . \
        -shared \
        -fPIC \
        $JAVA_INC \
        -I /netopt/ncbi_tools64/c++.stable/include \
        -I /netopt/ncbi_tools64/c++.stable/GCC493-ReleaseMT/inc \
        -L /netopt/ncbi_tools64/c++.stable/GCC493-ReleaseMT/lib \
        -L . \
	-L ext \
        -fopenmp -lxblastformat -lalign_format -ltaxon1 -lblastdb_format -lgene_info -lxformat -lxcleanup -lgbseq -lmlacli -lmla -lmedlars -lpubmed -lvalid -ltaxon3 -lxalnmgr -lblastxml -lblastxml2 -lxcgi -lxhtml -lproteinkmer -lxblast -lxalgoblastdbindex -lcomposition_adjustment -lxalgodustmask -lxalgowinmask -lseqmasks_io -lseqdb -lblast_services -lxalnmgr -lxobjutil -lxobjread -lvariation -lcreaders -lsubmit -lxnetblastcli -lxnetblast -lblastdb -lscoremat -ltables -lxregexp -lncbi_xloader_genbank -lncbi_xreader_id1 -lncbi_xreader_id2 -lncbi_xreader_cache -lncbi_xreader_pubseqos -ldbapi_driver -lncbi_xreader -lxconnext -lxconnect -lid1 -lid2 -lxobjmgr -lgenome_collection -lseqedit -lseqsplit -lsubmit -lseqset -lseq -lseqcode -lsequtil -lpub -lmedline -lbiblio -lgeneral -lxser -lxutil -lxncbi -lxcompress -llmdb -lpthread -lz -lbz2 -L/netopt/ncbi_tools64/lzo-2.05/lib64 -llzo2 -ldl -lz -lnsl -ldw -lrt -ldl -lm -lpthread\
        -o blastjni.so
    mkdir -p /tmp/blast
    cp -n /netopt/ncbi_tools64/lmdb-0.9.21/lib/*so ext
fi

cp blastjni.so /tmp/blast
cp ext/liblmdb.so /tmp/blast/

echo "Testing JNI"
java -Djava.library.path=$PWD -cp . gov.nih.nlm.ncbi.blastjni.BlastJNI > test.result 2>&1
set +errexit
CMP=$(cmp test.result test.expected)
if [[ $? -ne 0 ]]; then
    echo "Test failed"
    sdiff -w 70 test.result test.expected
    exit 1
fi
set -o errexit
echo "Test OK"

if [ "$ONGCP" = "false" ]; then
    echo "Compiling test_blast.cpp"
    rm -f test_blast
    g++ test_blast.cpp -L./int/blast/libs \
        -std=gnu++11 \
        -Wall -g -fPIC -I . \
        -I /netopt/ncbi_tools64/c++.stable/include \
        -I /netopt/ncbi_tools64/c++.stable/GCC493-ReleaseMT/inc \
        -L /netopt/ncbi_tools64/c++.stable/GCC493-ReleaseMT/lib \
        -L . \
        -L ext \
        -fopenmp -lxblastformat -lalign_format -ltaxon1 -lblastdb_format -lgene_info -lxformat -lxcleanup -lgbseq -lmlacli -lmla -lmedlars -lpubmed -lvalid -ltaxon3 -lxalnmgr -lblastxml -lblastxml2 -lxcgi -lxhtml -lproteinkmer -lxblast -lxalgoblastdbindex -lcomposition_adjustment -lxalgodustmask -lxalgowinmask -lseqmasks_io -lseqdb -lblast_services -lxalnmgr -lxobjutil -lxobjread -lvariation -lcreaders -lsubmit -lxnetblastcli -lxnetblast -lblastdb -lscoremat -ltables -lxregexp -lncbi_xloader_genbank -lncbi_xreader_id1 -lncbi_xreader_id2 -lncbi_xreader_cache -lncbi_xreader_pubseqos -ldbapi_driver -lncbi_xreader -lxconnext -lxconnect -lid1 -lid2 -lxobjmgr -lgenome_collection -lseqedit -lseqsplit -lsubmit -lseqset -lseq -lseqcode -lsequtil -lpub -lmedline -lbiblio -lgeneral -lxser -lxutil -lxncbi -lxcompress -llmdb -lpthread -lz -lbz2 -L/netopt/ncbi_tools64/lzo-2.05/lib64 -llzo2 -ldl -lz -lnsl -ldw -lrt -ldl -lm -lpthread\
        -o test_blast

fi

echo "Testing Blast Library"
# More tests at https://www.ncbi.nlm.nih.gov/nuccore/JN166001.1?report=fasta

./test_blast 1 \
CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG \
nt.04 blastn > blast_test.result
set +errexit
CMP=$(cmp blast_test.result blast_test.expected)
if [[ $? -ne 0 ]]; then
    echo "Test failed"
    sdiff -w 70 blast_test.result blast_test.expected | head
    exit 1
fi
set -o errexit
echo "Test OK"

echo "Maven packaging"
mvn package
ls -l target/*jar

echo "Make_partitions.py"
./make_partitions.py > db_partitions.jsonl

echo "Creating JAR"
jar cf BlastJNI.jar gov blastjni.so
unzip -v BlastJNI.jar

if [ "$ONGCP" = "true" ]; then
    echo "Copying to Cloud Storage Bucket"
    gsutil cp \
        cluster_initialize.sh \
        "$PIPELINEBUCKET/scipts/cluster_initialize.sh"

    gsutil cp \
        db_partitions.jsonl \
        "$PIPELINEBUCKET/dbs/db_partitions.jsonl"

    gsutil cp \
        ext/liblmdb.so \
       "$PIPELINEBUCKET/libs/liblmdb.so"

    gsutil cp \
        blastjni.so \
	"$PIPELINEBUCKET/libs/blastjni.so"

    gsutil cp \
        query.jsonl \
        "$PIPELINEBUCKET/input/query.jsonl"
	
    # gsutil ls -l -r "$PIPELINEBUCKET/"
fi


echo "Build Complete"
exit 0
wc << HINTS
 gcloud auth login (copy/paste from web)
 gcloud dataproc clusters list  --region=us-east4
 gcloud dataproc jobs list
 gcloud dataproc jobs submit spark --cluster XXX --jar foo.jar arg1 arg2
 gcloud dataproc jobs submit spark --cluster cluster-blast-vartanianmh --class org.apache.spark.examples.SparkPi --region=us-east4

 gcloud dataproc jobs submit spark --cluster cluster-blast-vartanianmh --class org.apache.spark.examples.SparkPi --jars file:///usr/lib/spark/examples/jars/spark-examples.jar  --region=us-east4 --max-failures-per-hour 2

gcloud dataproc --region us-east4 \
    clusters create cluster-3941 \
    --subnet t --zone us-east4-b \ # --zone "" ?
    --master-machine-type n1-standard-4 --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --project ncbi-sandbox-blast \
    --labels owner=vartanianmh \
    --initialization-actions \
    'gs://blastgcp-pipeline-test/scipts/cluster_initialize.sh' \
    --initialization-actions-timeout 60 # Default 10m \
    --max-age=8h

    git clone https://github.com/ncbi/blast-gcp.git
    cd blast-gcp
    git checkout engineering
    git config --global user.email "mike.vartanian@nih.gov"
    git config --global user.name "Mike Vartanian"


# Can be useful for debugging
 export SPARK_PRINT_LAUNCH_COMMAND=1

# To manually populate /tmp on workers:
 cd /tmp;gsutil cp gs://blastgcp-pipeline-test/scipts/cluster_initialize.sh .;chmod +x cluster_initialize.sh; sudo ./cluster_initialize.sh

# Not sure if needed
 sudo vi /etc/spark/conf.dist/spark-env.sh
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/blast
# could be replaced by
 -conf spark.executorEnv.LD_LIBRARY_PATH="/tmp/blast"

 spark-submit \
     --conf spark.executorEnv.LD_LIBRARY_PATH="/tmp/blast" \
     --files blastjni.so  \
     --jars BlastJNI.jar \
     --class BlastSpark \
     --master yarn \
     target/blastjni-0.0314.jar \
	$PIPELINEBUCKET/input/query.jsonl
     #/user/vartanianmh/query.jsonl


HINTS


