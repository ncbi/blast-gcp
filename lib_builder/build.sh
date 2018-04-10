#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o errexit

tput reset

PIPELINEBUCKET="gs://blastgcp-pipeline-test"

set +errexit
distro=$(grep Debian /etc/os-release | wc -l)
set -o errexit
if [ "$distro" -ne 0 ]; then
    export DISTRO="Debian 8"
    export BUILDENV="google"
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
    export BLASTDB=/tmp/blast/
    export SPARK_HOME=/usr/lib/spark/
    export LD_LIBRARY_PATH=".:$PWD/ext"
else
    export DISTRO="CentOS 7"
    export BUILDENV="ncbi"
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
    #export LD_LIBRARY_PATH=".:/opt/ncbi/gcc/4.9.3/lib64/:/home/vartanianmh/blast-gcp/pipeline/ext"
    export LD_LIBRARY_PATH=".:/opt/ncbi/gcc/4.9.3/lib64/:$PWD/ext"
    export BLASTDB=/net/frosty/vol/blast/db/blast
#    BLASTBYDATE=/netopt/ncbi_tools64/c++.stable/
#    BLASTBYDATE=/netopt/ncbi_tools64/c++.by-date/20180319/
    BLASTBYDATE="/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++/"
    export SPARK_HOME=/usr/local/spark/2.2.0/

fi

echo "Building at $BUILDENV on $DISTRO"

JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
export CLASSPATH="."

rm -f *.class
rm -f blastjni.o
rm -rf gov
rm -f *test.result
rm -f *.jar
rm -f /tmp/blast*.log
rm -f signatures
rm -f core.* hs_err_* output.*


# TODO: Unfortunately, BlastJNI.h can only be built @ Google, due to
#packages,  but is required by g++ # at NCBI. Revisit after Jira BG-21
DEPENDS="$SPARK_HOME/jars/*:."
MAIN_JAR="sprint3.jar"
echo "Compiling Java and creating JNI header"
#NOTE: javah deprecated in Java 9, removed in Java 10
javac -Xlint:all -Xlint:-path -Xlint:-serial -cp $DEPENDS:. -d . -h . \
    BLAST_REQUEST.java \
    BLAST_PARTITION.java \
    BLAST_HSP_LIST.java \
    BLAST_TB_LIST.java \
    BLAST_LIB.java
javap -p -s gov/nih/nlm/ncbi/blastjni/BLAST_LIB.class >> signatures
javap -p -s gov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST.class >> signatures
javap -p -s gov/nih/nlm/ncbi/blastjni/BLAST_TB_LIST.class >> signatures

echo "Creating JAR"
jar cf $MAIN_JAR gov/nih/nlm/ncbi/blastjni/*class
rm -rf gov

if [ "$BUILDENV" = "ncbi" ]; then
    echo "Compiling and linking blastjni.cpp"
    # Note: Library order important
    #       lmdb previously built at NCBI as static .a in /ext/
    #       Hidden dl_open for libdw
    # Eugene has:
    #        -static-libstdc++  # Needed for NCBI's Spark cluster (RHEL7?)
        #-ldbapi_driver -lncbi_xreader \
    g++ blastjni.cpp \
        -std=gnu++11 \
        -Wall -O  -I . \
        -shared \
        -fPIC \
        $JAVA_INC \
        -L./int/blast/libs \
        -I $BLASTBYDATE/include \
        -I $BLASTBYDATE/ReleaseMT/inc \
        -L $BLASTBYDATE/ReleaseMT/lib \
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
        -lncbi_xreader \
        -lncbi_xreader_id2 \
        -lxconnect -lid1 -lid2 -lxobjmgr \
        -lgenome_collection -lseqedit -lseqsplit -lsubmit \
        -lseqset -lseq -lseqcode -lsequtil -lpub -lmedline \
        -lbiblio -lgeneral -lxser -lxutil -lxncbi -lxcompress \
        -llmdb -lpthread -lz -lbz2 \
        -L/netopt/ncbi_tools64/lzo-2.05/lib64 \
        -llzo2 -ldl -lz -lnsl -ldw -lrt -ldl -lm -lpthread \
        -o libblastjni.so
fi


#if [ "$BUILDENV" = "google" ]; then
    echo "Testing JNI"
    set +errexit
    ldd libblastjni.so | grep found
    if [[ $? -ne 1 ]]; then
        echo "Missing a shared library"
        echo "LD_LIBRARY_PATH is $LD_LIBRARY_PATH"
        exit 1
    fi
        #-verbose:jni \
    java -Djava.library.path="." \
    -Xcheck:jni -Xdiag -Xfuture \
        -cp $MAIN_JAR:.  \
        gov.nih.nlm.ncbi.blastjni.BLAST_LIB \
        > output.$$ 2>&1
    sort output.$$ | grep -e "000 " > test.result
    CMP=$(cmp test.result test.expected)
    if [[ $? -ne 0 ]]; then
        cat -tn output.$$
        #rm -f output.$$
        sdiff -w 70 test.result test.expected
        echo "Testing of JNI failed"
        exit 1
    fi
    rm -f output.$$
    set -o errexit
    echo "Test OK"
#fi

if [ "1" = "0" ] && [ "$BUILDENV" = "ncbi" ]; then
    echo "Compiling and linking test_blast.cpp"
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


# TODO: Can this be run in both environments?
if [ "1" = "0" ]  && [ "$BUILDENV" = "ncbi" ]; then
    echo "Testing Blast Library"
    # More tests at https://www.ncbi.nlm.nih.gov/nuccore/JN166001.1?report=fasta
        ./test_blast 1 \
        CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG \
        nt.04 blastn > blast_test.result
        set +errexit
        CMP=$(cmp blast_test.result blast_test.expected)
        if [[ $? -ne 0 ]]; then
            sdiff -w 70 blast_test.result blast_test.expected | head
            echo "Testing Blast Library failed"
            exit 1
        fi
        set -o errexit
    echo "Test OK"
fi

if [ "$BUILDENV" = "google" ]; then
    echo "Copying to Cloud Storage Bucket"
    gsutil cp \
        cluster_initialize.sh \
        "$PIPELINEBUCKET/scripts/cluster_initialize.sh"
fi

mv libblastjni.so ../pipeline
echo "Build Complete"
date
echo
exit 0




<<HINTS
gcloud auth application-default login --no-launch-browser

git clone https://github.com/ncbi/blast-gcp.git
cd blast-gcp
git checkout engineering
sudo apt-get install libdw-dev -y
git config --global user.email "Mike.Vartanian@nih.gov"
git config --global user.name "Mike Vartanian"


# Can be useful for debugging
 export SPARK_PRINT_LAUNCH_COMMAND=1

# To manually populate /tmp on workers:
 cd /tmp;gsutil cp gs://blastgcp-pipeline-test/scripts/cluster_initialize.sh .;chmod +x cluster_initialize.sh; sudo ./cluster_initialize.sh

# Not sure if needed
 sudo vi /etc/spark/conf.dist/spark-env.sh
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/blast
# could be replaced by
 -conf spark.executorEnv.LD_LIBRARY_PATH="/tmp/blast"


$ yarn logs --allicationID <appID>
yarn.nodemanager.delete.debug-delay-sec property Spark History Server
 with the yarn.log-aggregation-enable config
 Once the job has completed the NodeManager will keep the log for each
 container for ${yarn.nodemanager.log.retain-seconds} which is
 10800 seconds by default ( 3 hours ) and delete them once they have expired.
 But if ${yarn.log-aggregation-enable} is enabled then the NodeManager
 will immediately concatenate all of the containers logs into one file
 and upload them into HDFS in
   ${yarn.nodemanager.remote-app-log-dir}/${user.name}/logs/ and delete
   them from the local userlogs directory




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


