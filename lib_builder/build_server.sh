#!/usr/bin/env bash
set -o nounset # same as -u
set -o errexit # same as -e
set -o pipefail
shopt -s nullglob globstar #

function line() {
    echo "---------------------------------------------"
}

export LD_LIBRARY_PATH=".:../pipeline"
export DISTRO="CentOS 7"
export BUILDENV="ncbi"
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"
export BLASTDB=/net/frosty/vol/blast/db/blast
BLASTBYDATE="/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++/"
export SPARK_HOME=/usr/local/spark/2.2.0/

echo "Building at $BUILDENV on $DISTRO"

JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
export CLASSPATH="."

BLAST_SERVER_GPP_COMMAND="
    g++ \
    blast_server.cpp \
    -std=gnu++11 \
    -march=sandybridge \
    -Wall -O3 -I . \
    -Wextra -pedantic \
    -Wlogical-op \
    -Wshadow \
    -Wformat=2 \
    -Wformat-security \
    -Woverloaded-virtual \
    -Wcast-align \
    -Wno-ctor-dtor-privacy \
    -Wdisabled-optimization \
    -Winit-self \
    -Wmissing-declarations \
    -Wmissing-include-dirs \
    -Wredundant-decls \
    -Wsign-promo \
    -Wstrict-overflow=5 \
    -Wswitch \
    -Wno-unused \
    -Wnon-virtual-dtor \
    -Wreorder \
    -Wdeprecated \
    -Wno-float-equal \
    -fPIC \
    -L./int/blast/libs \
    -I $BLASTBYDATE/include \
    $JAVA_INC \
    -I $BLASTBYDATE/ReleaseMT/inc \
    -L $BLASTBYDATE/ReleaseMT/lib \
    -I/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/lmdb-0.9.21 \
    -L/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/lmdb-0.9.21 \
    -L . \
    -L ext \
    -L/netopt/ncbi_tools64/lzo-2.05/lib64 \
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
    -o blast_server"

echo "Running static analysis on C++ code"
cppcheck -U_MSC_VER -U__IBMCPP__ -q --enable=all --platform=unix64 --std=c++11 blast_server.cpp
scan-build --use-analyzer /usr/local/llvm/3.8.0/bin/clang "$BLAST_SERVER_GPP_COMMAND"
echo "Static analysis on C++ code complete"

$BLAST_SERVER_GPP_COMMAND
cp blast_server ../pipeline

line
set +errexit
echo "Running tests..."
./blast_server 12345
cat blast_server.test.json | nc localhost 12345 > blast_server.test.result
killall blast_server
cmp blast_server.test.result blast_server.test.expected
if [[ $? -ne 0 ]]; then
    sdiff -w 70 blast_server.test.result blast_server.test.expected | head
    echo "  Testing of blast_server failed"
    exit 1
fi

echo "Tests complete"
line

echo "Build Complete"
date
echo
exit 0

