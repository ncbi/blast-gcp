#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

set +errexit
distro=$(grep Debian /etc/os-release | wc -l)
set -o errexit
if [ "$distro" -ne 0 ]; then
    export DISTRO="Debian 8"
    export BUILDENV="google"
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
    export BLASTDB=/tmp/blast/db
else
    export DISTRO="CentOS 7"
    export BUILDENV="ncbi"
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
    export LD_LIBRARY_PATH=".:/opt/ncbi/gcc/4.9.3/lib64/"
    export BLASTDB=/net/frosty/vol/blast/db/blast
    BLASTBYDATE=/netopt/ncbi_tools64/c++.stable/
#    BLASTBYDATE=/netopt/ncbi_tools64/c++.by-date/20180319/
fi

echo "Building at $BUILDENV on $DISTRO"

JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
export CLASSPATH="."

rm -f BlastJNI.class
rm -f blastjni.o
rm -rf gov
rm -f *test.result
rm -f BlastJNI.jar


echo "Maven packaging"
    mvn package
    mvn assembly:assembly -DdescriptorId=jar-with-dependencies
    ls -l target/*jar

# TODO: Unfortunately, BlastJNI.h can only be built @ Google, due to 
#packages,  but is required by g++ # at NCBI.
HDR="BlastJNI.h"
if [ "$BUILDENV" = "google" ]; then
    echo "Creating BlastJNI header: $HDR"
    #javac -d . -h . src/main/java/BlastJNI.java
    javac -cp target/blastjni-0.0314-jar-with-dependencies.jar  -d . -h . src/main/java/BlastJNI.java
    echo "/*" >> $HDR
    echo "$USER" >> $HDR
    javac -version >> $HDR 2>&1
    g++ --version | head -1 >> $HDR
    echo "*/" >> $HDR
#else
#    # Can work at NCBI if target/ present:
#    if [ -d target ]; then
#        echo "Creating BlastJNI header"
#       javac -cp target/blastjni-0.0314-jar-with-dependencies.jar  -d . -h . src/main/java/BlastJNI.java
#    fi
fi

if [ "$BUILDENV" = "ncbi" ]; then
    echo "Compiling blastjni.cpp"
    # Note: Library order important
    #       lmdb previously built at NCBI as static .a in /ext/
    #       Hidden dl_open for libdw
    g++ blastjni.cpp \
        -L./int/blast/libs \
        -std=gnu++11 \
        -Wall -g  -I . \
        -shared \
        -fPIC \
        $JAVA_INC \
        -I $BLASTBYDATE/include \
        -I $BLASTBYDATE/GCC493-ReleaseMT/inc \
        -L $BLASTBYDATE/GCC493-ReleaseMT/lib \
        -L . \
        -L ext \
        -fopenmp -lxblastformat -lalign_format -ltaxon1 -lblastdb_format \
        -lgene_info -lxformat -lxcleanup -lgbseq -lmlacli \
        -lmla -lmedlars -lpubmed -lvalid -ltaxon3 -lxalnmgr \
        -lblastxml -lblastxml2 -lxcgi -lxhtml -lproteinkmer \
        -lxblast -lxalgoblastdbindex -lcomposition_adjustment \
        -lxalgodustmask -lxalgowinmask -lseqmasks_io -lseqdb \
        -lblast_services -lxalnmgr -lxobjutil -lxobjread \
        -lvariation -lcreaders -lsubmit -lxnetblastcli \
        -lxnetblast -lblastdb -lscoremat -ltables -lxregexp \
        -lncbi_xloader_genbank -lncbi_xreader_id1 \
        -lncbi_xreader_id2 -lncbi_xreader_cache \
        -lncbi_xreader_pubseqos -ldbapi_driver -lncbi_xreader \
        -lxconnext -lxconnect -lid1 -lid2 -lxobjmgr \
        -lgenome_collection -lseqedit -lseqsplit -lsubmit \
        -lseqset -lseq -lseqcode -lsequtil -lpub -lmedline \
        -lbiblio -lgeneral -lxser -lxutil -lxncbi -lxcompress \
        -llmdb -lpthread -lz -lbz2 \
        -L/netopt/ncbi_tools64/lzo-2.05/lib64 \
        -llzo2 -ldl -lz -lnsl -ldw -lrt -ldl -lm -lpthread \
        -o libblastjni.so
fi


echo "Testing JNI"
    #java -cp target/blastjni-0.0314.jar BlastJNI
    java -cp target/blastjni-0.0314-jar-with-dependencies.jar BlastJNI
    java -Djava.library.path=$PWD -cp . BlastJNI > test.result 2>&1
    set +errexit
    CMP=$(cmp test.result test.expected)
    if [[ $? -ne 0 ]]; then
        echo "Test failed"
        sdiff -w 70 test.result test.expected
        exit 1
    fi
    set -o errexit
echo "Test OK"

if [ "$BUILDENV" = "ncbi" ]; then
    echo "Compiling test_blast.cpp"
    rm -f test_blast
    g++ test_blast.cpp -L./int/blast/libs \
        -std=gnu++11 \
        -Wall -g -fPIC -I . \
        -I $BLASTBYDATE/include \
        -I $BLASTBYDATE/GCC493-ReleaseMT/inc \
        -L $BLASTBYDATE/GCC493-ReleaseMT/lib \
        -L . \
        -L ext \
        -fopenmp -lxblastformat -lalign_format -ltaxon1 -lblastdb_format \
        -lgene_info -lxformat -lxcleanup -lgbseq -lmlacli \
        -lmla -lmedlars -lpubmed -lvalid -ltaxon3 -lxalnmgr \
        -lblastxml -lblastxml2 -lxcgi -lxhtml -lproteinkmer \
        -lxblast -lxalgoblastdbindex -lcomposition_adjustment \
        -lxalgodustmask -lxalgowinmask -lseqmasks_io -lseqdb \
        -lblast_services -lxalnmgr -lxobjutil -lxobjread \
        -lvariation -lcreaders -lsubmit -lxnetblastcli \
        -lxnetblast -lblastdb -lscoremat -ltables -lxregexp \
        -lncbi_xloader_genbank -lncbi_xreader_id1 \
        -lncbi_xreader_id2 -lncbi_xreader_cache \
        -lncbi_xreader_pubseqos -ldbapi_driver -lncbi_xreader \
        -lxconnext -lxconnect -lid1 -lid2 -lxobjmgr \
        -lgenome_collection -lseqedit -lseqsplit -lsubmit \
        -lseqset -lseq -lseqcode -lsequtil -lpub -lmedline \
        -lbiblio -lgeneral -lxser -lxutil -lxncbi -lxcompress \
        -llmdb -lpthread -lz -lbz2 \
        -L/netopt/ncbi_tools64/lzo-2.05/lib64 \
        -llzo2 -ldl -lz -lnsl -ldw -lrt -ldl -lm -lpthread \
        -o test_blast
fi

echo "Compiling BlastJNI Java"
javac -d . BlastJNI.java


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

echo "Make_partitions.py"
    ./make_partitions.py > db_partitions.jsonl

echo "Creating JAR"
    jar cf BlastJNI.jar BlastJNI.class libblastjni.so
    unzip -v BlastJNI.jar

if [ "$BUILDENV" = "google" ]; then
    echo "Copying to Cloud Storage Bucket"
    gsutil cp \
        cluster_initialize.sh \
        "$PIPELINEBUCKET/scipts/cluster_initialize.sh"

    gsutil cp \
        db_partitions.jsonl \
        "$PIPELINEBUCKET/dbs/db_partitions.jsonl"

    gsutil cp \
        query.jsonl \
        "$PIPELINEBUCKET/input/query.jsonl"
fi

echo "Build Complete"
date
exit 0




<<HINTS
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
    --tags ${USER}-dataproc-cluster-$(date +%Y%m%d-%H%M%S)


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

# gcloud dataproc clusters diagnose cluster-name


# gcloud dataproc clusters create args --single-node

# gcloud dataproc jobs submit <job type> args --max-failures-per-hour number

$ yarn logs
yarn.nodemanager.delete.debug-delay-sec property Spark History Server


 spark-submit \
     --conf spark.executorEnv.LD_LIBRARY_PATH="/tmp/blast" \
     --files libblastjni.so  \
     --jars BlastJNI.jar \
     --class BlastSpark \
     --master yarn \
     target/blastjni-0.0314.jar \
	$PIPELINEBUCKET/input/query.jsonl
     #/user/vartanianmh/query.jsonl


HINTS


