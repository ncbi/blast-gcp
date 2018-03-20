#!/bin/bash

#
# in other terminal on same machine call 'nc -lk 9999'
# for cluster with multiple worker node start' ncat -lkv 9999' on the driver
# obtain ncat via 'sudo apt-get install nmap'
#

SPARK_HOME="/usr/lib/spark/jars"
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

BLASTJNI="gov.nih.nlm.ncbi.blastjni.BlastJNI"
DEPENDS="$SPARK_HOME/*:$BLASTJNI:."

MAIN_JAR="./target/blastjni-0.0314.jar"
MAIN_CLASS="GCP_BLAST"
MASTER_L="local[4]"
MASTER_Y="yarn"
MASTER=$MASTER_Y

run_submit()
{
  spark-submit \
     --files libblastjni.so  \
     --deploy-mode client \
     --jars $MAIN_JAR \
     --master $MASTER \
     --class $MAIN_CLASS \
	foo.jar
}

run_submit2()
{
  spark-submit \
    --files libblastjni.so \
    --jars $MAIN_JAR \
    --class $MAIN_CLASS \
      foo.jar
}

compile_blast_java()
{
    rm -rf *.class
    echo "compiling java-classes"
    javac -Xlint:unchecked -cp $DEPENDS -d . src/*.java
    jar cf foo.jar *class
}

compile_blast_java
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "compile_blast_java failed"
    exit
fi
echo "compile_blast_java success"

rm -rf target
mvn package
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "mvn package failed"
    exit
fi
echo "mvn package success"

run_submit2

