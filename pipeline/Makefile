# Makefile for pipeline code
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Fri 23 Mar 2018 01:14:54 PM EDT
# Tested at NCBI only

.PHONY: all clean distclean check
BLASTBYDATE=/netopt/ncbi_tools64/c++.stable/

JAR_VERSION=$(shell grep version pom.xml  | grep -v xml | head -1 | sed 's/<version>//;s,</version>,,' | tr -d ' ')
JAVA_SRC=$(shell find src/main/java -type f -name "*.java")
DESC_ID=jar-with-dependencies
JAR=target/blastjni-$(JAR_VERSION).jar
FAT_JAR=target/blastjni-$(JAR_VERSION)-${DESC_ID}.jar

NCBI_CPP_TOOLKIT_INCLUDES=-I. -I${BLASTBYDATE}/include -I${BLASTBYDATE}/GCC493-ReleaseMT/inc
JAVA_INC=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux
CXX_FLAGS= -Wall -g -std=gnu++11 -fPIC 
#       lmdb previously built at NCBI as static .a in /ext/
#       Hidden dl_open for libdw
LD_FLAGS=-L./int/blast/libs -L${BLASTBYDATE}/GCC493-ReleaseMT/lib -L. -L ext
LIBS=\
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

all: ${JAR} ${FAT_JAR} test_blast libblastjni.so db_partitions.jsonl check

${JAR}: ${JAVA_SRC}
	mvn -q package

${FAT_JAR}: ${JAVA_SRC}
	mvn -q assembly:assembly -DdescriptorId=${DESC_ID}

BlastJNI.h: src/main/java/BlastJNI.java ${FAT_JAR}
	javah -cp ${FAT_JAR} -o $@ $(subst .h,,$@)

libblastjni.so: blastjni.cpp BlastJNI.h
	g++ $< ${CXX_FLAGS} -shared ${NCBI_CPP_TOOLKIT_INCLUDES} ${JAVA_INC} ${LD_FLAGS} ${LIBS} -o $@

test_blast: test_blast.cpp blast4spark.hpp
	g++ $< ${CXX_FLAGS} ${NCBI_CPP_TOOLKIT_INCLUDES} ${LD_FLAGS} ${LIBS} -o $@

check: blast_test.result
	cmp $< blast_test.expected

blast_test.result: test_blast
	./$< 1 \
        CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG \
        nt.04 blastn > $@

strace_check: BLASTDB=/net/snowman/vol/export2/camacho/small-blastdbs/seq-data-only
strace_check:
	strace -CtttTy -o strace.out ./test_blast 1 \
        CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG \
        nt.53 blastn > blast_test.result
	grep open strace.out | grep nt.53

db_partitions.jsonl:
	./make_partitions.py > $@
env: requirements.txt
	virtualenv -p python3 env
	. env/bin/activate && pip install -r requirements.txt

clean:
	$(RM) -r test_blast *so 
	git co libblastjni.so test_blast

distclean: clean
	${RM} -r env ${JAR} ${FAT_JAR}
	${RM} cscope.* tags vim.paths

#########################################################################
# Code navigation aids

cscope.files:
	[ -s $@ ] || ack -f --sort-files --cc --cpp --java | xargs realpath > $@

tags: cscope.files
	ctags -L $^

cscope.out: cscope.files
	cscope -bq

vim.paths: cscope.files
	xargs -n1 -I{} dirname {} < $^ | sort -u | sed 's/^/set path+=/' > $@

