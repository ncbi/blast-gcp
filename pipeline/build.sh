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
#    JAVA_INC="-I$JAVA_HOME/include"
else
    echo "Building at NCBI on CentOS 7"
    export ONGCP="false"
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
fi
JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
export CLASSPATH="."

HDR="gov_nih_nlm_ncbi_blastjni_BlastJNI.h"
#rm -rf /tmp/blast
rm -f $HDR
rm -f BlastJNI.class
rm -f blastjni.o
#rm -rf blast-libs
rm -rf gov
rm -f *test.result
rm -rf target
rm -f BlastJNI.jar
rm -f db_partitions.json db_partitions.jsonl
#rm -f src.zip
#rm -f blast4spar*pp

#curl -o blast4spark.cpp https://svn.ncbi.nlm.nih.gov/viewvc/toolkit/branches/blast_gcp/src/algo/blast/api/blast4spark.cpp?view=co&content-type=text%2Fplain
#curl -o blast4spark.hpp https://svn.ncbi.nlm.nih.gov/viewvc/toolkit/branches/blast_gcp/include/algo/blast/api/blast4spark.hpp?view=co&content-type=text%2Fplain
if [ "$ONGCP" = "false" ]; then
    rm -f test_blast
    cp -n /netopt/ncbi_tools64/lmdb-0.9.21/lib/*so ext
    echo "Compiling test_blast.cpp"
    g++ test_blast.cpp -L./blast-libs \
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

if [ "$ONGCP" = "false" ]; then
    export BLASTDB=/net/napme02/vol/blast/db/blast
else
    export BLASTDB=/tmp/blast/db
fi
export LD_LIBRARY_PATH="/tmp/blast:.:/opt/ncbi/gcc/4.9.3/lib64/"
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

echo "Compiling BlastJNI.java"
mkdir -p gov/nih/nlm/ncbi
javac -d gov/nih/nlm/ncbi -h . BlastJNI.java
rm -rf gov
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
        -L./blast-libs \
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
    # TODO: llmdb-static?
    mkdir -p /tmp/blast
    cp blastjni.so /tmp/blast

    if [ ! -d blast-libs ]; then
        mkdir -p blast-libs
        cd blast-libs
        tar -xf ~/blast-libs.tgz
        cd ..
    fi
fi

echo "Testing JNI"
java -Djava.library.path=$PWD -cp . gov.nih.nlm.ncbi.blastjni.BlastJNI > test.result
set +errexit
CMP=$(cmp test.result test.expected)
if [[ $? -ne 0 ]]; then
    echo "Test failed"
    sdiff -w 70 test.result test.expected
    exit 1
fi
set -o errexit
echo "Test OK"

echo "Maven packaging"
mvn package
ls -l target/*jar

echo "Make_partitions.py"
./make_partitions.py > db_partitions.jsonl

#echo "Zipping source"
#zip -q -r src.zip \
#      build.sh BlastJNI.java blastjni.cpp $HDR \
#      *.expected *py pom.xml \
#      src/ \
#      *.json *.jsonl \
#      test_blast.cpp \
#      blast4spark.hpp \
#      blastjni.so \
#      ext/liblmdb.so \
#      test_blast \
#      cluster_initialize.sh \
#      *.py

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
        "$PIPELINEBUCKET/db_partitions.jsonl"

    #gsutil cp \
    #    ext/liblmdb.so \
    #    "$PIPELINEBUCKET/liblmdb.so"

    #gsutil cp ~/nt.tar gs://blastgcp-pipeline-test/dbs/nt04.tar
    #gsutil cp liblmdb.so gs://blastgcp-pipeline-test/libs/liblmdb.so
    #gsutil cp blastjni.so gs://blastgcp-pipeline-test/libs/blastjni.so

    gsutil ls -l -r "$PIPELINEBUCKET/"
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

 gcloud beta dataproc --region us-east4 clusters create cluster-vartanianmh --subnet t --zone "" --master-machine-type n1-standard-1 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n1-standard-1 --worker-boot-disk-size 50 --project ncbi-sandbox-blast # TODO: --max-age=8h
 gcloud dataproc --region us-east4 clusters create cluster-1ad8 --subnet default --zone us-east4-b --master-machine-type n1-standard-1 --master-boot-disk-size 100 --num-workers 2 --worker-machine-type n1-standard-1 --worker-boot-disk-size 100 --num-preemptible-workers 1 --project ncbi-sandbox-blast
 --initialization-actions $PIPELINEBUCKET/scripts/cluster_initialize.sh

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
    'gs://blastgcp-pipeline-test/scipts/cluster_initialize.sh'
    --initialization-actions-timeout 60 # Default 10m


 sudo apt-get install maven libdw-dev -y #liblmdb0
 hadoop fs -copyFromLocal -f query.jsonl /user/vartanianmh/query.jsonl
 hadoop fs -copyFromLocal -f db_partitions.jsonl /user/vartanianmh/db_partitions.jsonl

 scp src.zip 35.188.236.53:/home/vartanianmh/blast/src.zip
 unzip -o src; \
 ./build.sh; \
 spark-submit --files blastjni.so \
 --jars BlastJNI.jar --class BlastSpark --master yarn \
 target/blastjni-0.0314.jar /user/vartanianmh/query.jsonl

 hadoop fs -rm -r map* foo* job* joi* csv*

 export SPARK_PRINT_LAUNCH_COMMAND=1

 cd /tmp;gsutil cp gs://blastgcp-pipeline-test/scipts/cluster_initialize.sh .;chmod +x cluster_initialize.sh; sudo ./cluster_initialize.sh

 vi /etc/spark/conf.dist/spark-env.sh
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/blast

 -conf spark.executorEnv.LD_LIBRARY_PATH="/tmp/blast"
 spark-submit --conf spark.executorEnv.LD_LIBRARY_PATH="/tmp/blast" \
     --files blastjni.so  --jars BlastJNI.jar \
     --class BlastSpark \
     --master yarn \
     target/blastjni-0.0314.jar \
     /user/vartanianmh/query.jsonl


HINTS


