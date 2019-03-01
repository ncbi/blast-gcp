#!/usr/bin/env bash
set -o nounset # same as -u
set -o errexit # same as -e
set -o pipefail
shopt -s nullglob globstar #

function line() {
    echo "---------------------------------------------"
}
function indent() {
    sed 's/^/    /';
}


PIPELINEBUCKET="gs://blastgcp-pipeline-test"

export LD_LIBRARY_PATH=".:../pipeline"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/ncbi/gcc/4.9.3/lib64/"
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v javac))))
export BLASTDB=/net/frosty/vol/blast/db/blast
export SPARK_HOME=/usr/local/spark/2.3.2/
export JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
export CLASSPATH="."
BLASTBYDATE="/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++/"
MAIN_JAR="../pipeline/target/sparkblast-1-jar-with-dependencies.jar"
DEPENDS="$SPARK_HOME/jars/*:$MAIN_JAR:."

set +errexit
rm -f ./*.class
rm -rf gov
rm -f ./*test.result
rm -f ./*.jar
rm -f /tmp/blastjni."$USER".log /tmp/blast_server.log
rm -f ./signatures
rm -f ./core.* ./hs_err_* ./output.*
rm -f libblastjni.o ../pipeline/libblastjni.so
rm -f output.*
rm -rf /tmp/scan-build-* /tmp/vartanianmh/scan-build-* > /dev/null 2>&1
set -o errexit


#if [ "1" == "1" ]; then
pushd ../pipeline > /dev/null
mvn -q clean
set +errexit
echo
echo "Running Java linters/static analyzers"
echo "    Running checkstyle"
mvn -q checkstyle:checkstyle > /dev/null 2>&1
#CHECKSTYLE="gs://blast-builds/checkstyle_sun.$TS.html"
#    gsutil cp target/site/checkstyle.html "$CHECKSTYLE"
cp target/site/checkstyle.html checkstyle.html
#    echo "  Output in $CHECKSTYLE"

    #mvn -q site > /dev/null 2>&1

    echo "    Running PMD:"
    "$HOME/pmd-bin-6.12.0/bin/run.sh" pmd -f textcolor -dir ./src/ -rulesets rulesets/java/quickstart.xml,category/java/codestyle.xml,category/java/bestpractices.xml > pmd.txt 2>/dev/null
    PMDLINES=$(wc -l < pmd.txt)
    echo "      PMD had $PMDLINES complaints"

    popd > /dev/null

    echo
    echo "Running clang-tidy checkers on C++ code"
    /usr/local/llvm/7.0.0/bin/clang-tidy -checks='*,-cppcoreguidelines-pro-bounds-pointer-arithmetic,-cppcoreguidelines-pro-type-vararg,-hicpp-vararg,-fuchsia-default-arguments,-cppcoreguidelines-pro-bounds-array-to-pointer-decay,-hicpp-no-array-decay' \
        blastjni.cpp -- \
        -std=c++14 \ -I.  -I/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-0.el7_5.x86_64/include -I/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-0.el7_5.x86_64/include/linux -I/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++//include -I/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++//ReleaseMT/inc -I/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/lmdb-0.9.21 -I/usr/include/c++/4.8.2 -I/usr/include/c++/4.8.2/bits -I/usr/include/c++/4.8.2/x86_64-redhat-linux -I/usr/include/c++/4.8.2/backward \
        2>&1 | indent

# FIX: Unfortunately, BlastJNI.h can only be built @ Google, due to
#packages,  but is required by g++ # at NCBI. Revisit after Jira BG-21

# Note: Library order important
#       Hidden dl_open for libdw
# Eugene has:
#        -static-libstdc++  # Needed for NCBI's Spark cluster (RHEL7?)
#-ldbapi_driver -lncbi_xreader \
#-Wundef \
#-Wswitch-enum \
#-Wdouble-promotion \
GPPCOMMAND="g++ \
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
    -llzo2 -ldl -lz -lnsl -lrt -ldl -lm -lpthread \
    -o ./libblastjni.so"

echo
echo "Running static analysis on C++ code"
cppcheck -q --enable=all --platform=unix64 blastjni.cpp  2>&1 | indent
#        cppcheck -q --enable=all --platform=unix64 --std=c++11 blast_json.cpp
scan-build --use-analyzer /usr/local/llvm/7.0.0/bin/clang "$GPPCOMMAND" 2>&1 | indent
echo "Static analysis on C++ code complete"

GCCVERSION=$(g++ --version | head -1)
echo "$GCCVERSION CLI is $GPPCOMMAND"

echo
echo "Compiling and linking blastjni.cpp"
$GPPCOMMAND 2>&1 | indent
cp libblastjni.so ../pipeline


echo "  Testing for unresolved libraries"
set +errexit
ldd ./libblastjni.so | grep found
if [[ $? -ne 1 ]]; then
    echo "Missing a shared library"
    echo "LD_LIBRARY_PATH is $LD_LIBRARY_PATH"
    exit 1
fi
echo "  Testing for unresolved libraries OK"

echo "  Unit testing C++ library"
pushd unit_test
make clean
make check
if [ $? -ne 0 ]; then
    echo "Failed unit test"
    exit 1
fi
echo "  Unit tested  C++ library"
popd

echo "Invoking mvn package"
pushd ../pipeline > /dev/null
mvn -q package 2>&1 | indent
popd > /dev/null
set -o errexit

#fi

echo
echo "Creating JNI header"
pushd ../pipeline > /dev/null
javac -Xlint:all -Xlint:-path -Xlint:-serial \
    -cp "$DEPENDS":.:src/main/java \
    -d . -h ../lib_builder \
    src/main/java/BLAST_LIB.java
    popd

    echo "  Generating function signatures"
    {
        javap -p -s ../pipeline/target/classes/gov/nih/nlm/ncbi/blastjni/BLAST_LIB.class
        javap -p -s ../pipeline/target/classes/gov/nih/nlm/ncbi/blastjni/BLAST_HSP_LIST.class
        javap -p -s ../pipeline/target/classes/gov/nih/nlm/ncbi/blastjni/BLAST_TB_LIST.class
    } >> signatures

line
set +errexit
echo "Running tests..."
echo "  Testing JNI function signatures"
md5sum -c signatures.md5 > /dev/null
echo "  Testing JNI function signatures OK"
#md5sum signatures > signatures.md5


echo "Compiling JAVA test harnesses"
javac -Xlint:all -Xlint:-path -Xlint:-serial -cp "$DEPENDS":. -d . -h . \
    ./BLAST_TEST.java
    #    javac -Xlint:all -Xlint:-path -Xlint:-serial -cp "$DEPENDS":. -d . -h . \
    #        ./BLAST_BENCH.java

   # if [ "1" == "1" ]; then
   echo "  Testing JNI"
   #-verbose:jni \
   #-Djava.library.path="../pipeline" \
   java \
       -Xcheck:jni -Xdiag -Xfuture \
       -cp $MAIN_JAR:. \
       gov.nih.nlm.ncbi.blastjni.BLAST_TEST \
       > output.$$ 2>&1

   sort -u output.$$ | grep -e "^HSP: " -e "^TB: " > test.result
   if ! cmp test.result test.expected
   then
       cat -tn output.$$
       sdiff -w 70 test.result test.expected
       echo "  Testing of JNI failed"
       exit 1
   fi
   echo "  Testing JNI OK"
   rm -f output.$$
   set -o errexit
   echo "Tests complete"

   echo "Build Complete"
   date
   echo
   exit 0

