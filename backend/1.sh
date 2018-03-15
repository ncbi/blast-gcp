#!/bin/bash

#
# in other terminal on same machine call 'nc -lk 9999'
#

SPARK_HOME="/usr/local/spark/2.2.0/jars"
JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"

BLASTJNI="gov.nih.nlm.ncbi.blastjni.BlastJNI"
DEPENDS="$SPARK_HOME/*:$BLASTJNI:."

MAIN_JAR="./gcp_blast.jar"
MAIN_CLASS="GCP_BLAST"

compile_blast_jni()
{
    rm -rf *.so
    echo "compiling and linking blastjni.so"
    g++ src/blastjni.cpp \
        -L ./int/blast/libs \
        -std=gnu++11 \
        -Wall \
        -g \
        -I . \
        -shared \
        -fPIC \
        -I $JAVA_HOME/include -I$JAVA_HOME/include/linux \
        -I /netopt/ncbi_tools64/c++.stable/include \
        -I /netopt/ncbi_tools64/c++.stable/GCC493-ReleaseMT/inc \
        -L /netopt/ncbi_tools64/c++.stable/GCC493-ReleaseMT/lib \
        -L . \
        -L ext \
        -fopenmp -lxblastformat -lalign_format -ltaxon1 -lblastdb_format \
        -lgene_info -lxformat -lxcleanup -lgbseq -lmlacli -lmla -lmedlars \
        -lpubmed -lvalid -ltaxon3 -lxalnmgr -lblastxml -lblastxml2 -lxcgi \
        -lxhtml -lproteinkmer -lxblast -lxalgoblastdbindex -lcomposition_adjustment \
        -lxalgodustmask -lxalgowinmask -lseqmasks_io -lseqdb -lblast_services \
        -lxalnmgr -lxobjutil -lxobjread -lvariation -lcreaders -lsubmit \
        -lxnetblastcli -lxnetblast -lblastdb -lscoremat -ltables -lxregexp \
        -lncbi_xloader_genbank -lncbi_xreader_id1 -lncbi_xreader_id2 \
        -lncbi_xreader_cache -lncbi_xreader_pubseqos -ldbapi_driver \
        -lncbi_xreader -lxconnext -lxconnect -lid1 -lid2 -lxobjmgr \
        -lgenome_collection -lseqedit -lseqsplit -lsubmit -lseqset -lseq \
        -lseqcode -lsequtil -lpub -lmedline -lbiblio -lgeneral -lxser -lxutil \
        -lxncbi -lxcompress -llmdb -lpthread -lz -lbz2 \
        -L/netopt/ncbi_tools64/lzo-2.05/lib64 -llzo2 -ldl -lz -lnsl -ldw -lrt -ldl -lm -lpthread \
        -o blastjni.so
}

compile_blast_java()
{
    rm -rf *.class
    echo "compiling java-classes"
	javac -Xlint:unchecked -cp $DEPENDS -d . src/*.java
}

package()
{
	#package all compiled classes into a jar
	#make the jar we package up contain all it's dependencies...
	jar cf $MAIN_JAR *.class
	rm -rf *.class
}

run_local()
{
    echo "running:"
	echo "java -cp $MAIN_JAR:$DEPENDS $MAIN_CLASS"
    export LD_LIBRARY_PATH=".:./ext:/opt/ncbi/gcc/4.9.3/lib64/"
    export BLASTDB="/net/napme02/vol/blast/db/blast"
	java -cp $MAIN_JAR:$DEPENDS $MAIN_CLASS
}

run_test()
{
    echo "running test:"
	echo "java -cp $MAIN_JAR:$DEPENDS BlastJNI"
    export LD_LIBRARY_PATH=".:./ext:/opt/ncbi/gcc/4.9.3/lib64/"
    export BLASTDB="/net/napme02/vol/blast/db/blast"
	java -cp $MAIN_JAR:$DEPENDS BlastJNI
}

compile_blast_jni
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "compile_blast_jni failed"
    exit
fi
echo "compile_blast_jni success"

compile_blast_java
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "compile_blast_java failed"
    exit
fi
echo "compile_blast_java success"

package
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "package jar failed"
    exit
fi
echo "package jar success"

#run_test
run_local
