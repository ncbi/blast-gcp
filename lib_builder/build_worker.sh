#!/usr/bin/env bash
set -o nounset # same as -u
set -o errexit # same as -e
set -o pipefail
shopt -s nullglob globstar #

function line() {
    echo "---------------------------------------------"
}


set +errexit
distro=$(grep -c Debian /etc/os-release)
set -o errexit
export LD_LIBRARY_PATH=".:../pipeline"
if [ "$distro" -ne 0 ]; then
    export DISTRO="Debian 8"
    export BUILDENV="google"
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
    export BLASTDB=/tmp/blast/
    export SPARK_HOME=/usr/lib/spark/
else
    export DISTRO="CentOS 7"
    export BUILDENV="ncbi"
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"
    export BLASTDB=/net/frosty/vol/blast/db/blast
    BLASTBYDATE="/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++/"
    export SPARK_HOME=/usr/local/spark/2.2.0/

fi

echo "Building at $BUILDENV on $DISTRO"

JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"

export MALLOC_CHECK_=2

    GPPCOMMAND="
    g++ \
    blastjni.cpp \
    -std=gnu++11 \
    -Wall -O -I . \
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
    -shared \
    -fPIC \
    $JAVA_INC \
    -L./int/blast/libs \
    -I $BLASTBYDATE/include \
    -I $BLASTBYDATE/ReleaseMT/inc \
    -L $BLASTBYDATE/ReleaseMT/lib \
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
    -llzo2 -ldl -lz -lnsl -lrt -ldl -lm -pthread \
    -o ./libblastjni.so"

    if [ "0" == "1" ]; then
        echo "Running static analysis on C++ code"
        cppcheck -q --enable=all --platform=unix64 --std=c++11 blastjni.cpp blast_worker.cpp
        scan-build --use-analyzer /usr/local/llvm/3.8.0/bin/clang "$GPPCOMMAND"
        echo "Static analysis on C++ code complete"
    fi

    echo "Compiling and linking blastjni.cpp"
    $GPPCOMMAND
    cp libblastjni.so ../pipeline

    g++ \
    blast_worker.cpp \
    -std=gnu++11 \
    -Wall -O -I . \
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
    -lblastjni \
    -L/netopt/ncbi_tools64/lzo-2.05/lib64 \
    -llzo2 -ldl -lz -lnsl -lrt -ldl -lm -lpthread \
    -o blast_worker


exit 0

