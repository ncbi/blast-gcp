#############################################################################
# Support for building shared library for github blast-gcp project
# Works at NCBI only
SPARK_HOME=/usr/local/spark/2.3.2/
BLASTBYDATE=/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++/
JAVA_INC=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux
MAIN_JAR="../pipeline/target/sparkblast-1-jar-with-dependencies.jar"
DEPENDS="${SPARK_HOME}/jars/*:${MAIN_JAR}:."

gov_nih_nlm_ncbi_blastjni_BLAST_LIB.h: ../pipeline/src/main/java/BLAST_LIB.java
	javac -Xlint:all -Xlint:-path -Xlint:-serial -cp "${DEPENDS}":. -d . -h . $^

libblastjni.so: blastjni.cpp blastjni.hpp
	g++ \
    $< \
    -std=gnu++11 \
    -Wall -O  -I . \
    -Wextra -pedantic \
    -Wlogical-op \
    -Wjump-misses-init \
    -Wshadow \
    -Wformat=2 \
    -Wformat-security \
    -Woverloaded-virtual \
    -shared \
    -fPIC \
    ${JAVA_INC} \
    -L./int/blast/libs \
    -I ${BLASTBYDATE}/include \
    -I ${BLASTBYDATE}/ReleaseMT/inc \
    -L ${BLASTBYDATE}/ReleaseMT/lib \
    -I/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/lmdb-0.9.21 \
    -L/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/lmdb-0.9.21 \
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
    -llmdb-static -lpthread -lz -lbz2 \
    -L/netopt/ncbi_tools64/lzo-2.05/lib64 \
    -llzo2 -ldl -lz -lnsl -lrt -ldl -lm -lpthread \
    -o $@

