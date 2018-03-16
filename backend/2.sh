#!/bin/bash

#
# in other terminal on same machine call 'nc -lk 9999'
#

SPARK_HOME="/usr/lib/spark/jars"
#JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"


BLASTJNI="gov.nih.nlm.ncbi.blastjni.BlastJNI"
DEPENDS="$SPARK_HOME/*:$BLASTJNI:."

#MAIN_JAR="./gcp_blast.jar"
MAIN_JAR="./target/blastjni-0.0314.jar"
MAIN_CLASS="GCP_BLAST"

run_local()
{
    echo "running:"
	echo "java -cp $MAIN_JAR:$DEPENDS $MAIN_CLASS"
    export LD_LIBRARY_PATH=".:./ext:/opt/ncbi/gcc/4.9.3/lib64/"
#    export BLASTDB="/net/napme02/vol/blast/db/blast"
	java -cp $MAIN_JAR:$DEPENDS:$HADOOP_CLASSPATH \
        $MAIN_CLASS  \
	yarn
}

run_submit()
{
#     --jars BlastJNI.jar,foo.jar,$MAIN_JAR \
  spark-submit \
     --files libblastjni.so  \
     --deploy-mode client \
     --jars foo.jar,$MAIN_JAR \
     --master yarn \
     --class GCP_BLAST \
	foo.jar
}

compile_blast_java()
{
    rm -rf *.class
    echo "compiling java-classes"
	javac -Xlint:unchecked -cp $DEPENDS -d . src/*.java
    jar cf foo.jar *class
}


#compile_blast_jni
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

mvn package
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "package jar failed"
    exit
fi
echo "package jar success"

#run_test

HADOOP_CLASSPATH=`hadoop classpath`
#run_local
run_submit
