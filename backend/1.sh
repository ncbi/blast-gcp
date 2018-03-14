#!/bin/bash

execute()
{
    echo $1
    eval $1
}

#
# in other terminal on same machine call 'nc -lk 9999'
#

#SPARK_HOME="$HOME/spark/JARS.2.2"
SPARK_HOME="/usr/local/spark/2.2.0/jars"
JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
JAVA_INC=" -I$JAVA_HOME/include -I$JAVA_HOME/include/linux"

BLASTJNI="gov.nih.nlm.ncbi.blastjni.BlastJNI"
DEPENDS="$SPARK_HOME/*:$BLASTJNI:."

MAIN_JAR="./gcp_blast.jar"
MAIN_CLASS="GCP_BLAST"

compile()
{
    echo "compiling:"
    
    #compile the JNI-interface
	rm -rf *.so  
    echo "g++ -Wall -g -fPIC -shared $JAVA_INC -o libblastjni.so .src/blastjni.cpp"
    g++ -Wall -g -fPIC -shared $JAVA_INC -o ./libblastjni.so ./src/blastjni.cpp

	#compile the java-sources into classes
	rm -rf *.class
    echo "javac -Xlint:unchecked -cp $DEPENDS -d . src/*.java"
	javac -Xlint:unchecked -cp $DEPENDS -d . src/*.java
}

package()
{
	#package all compiled classes into a jar ( for execution we still need classpaths to SPARK )
	#make the jar we package up contain all it's dependencies...
	jar cf $MAIN_JAR *.class
	rm -rf *.class
}

run_local()
{
    echo "run:"
	echo "java -cp $MAIN_JAR:$DEPENDS $MAIN_CLASS"
	java -cp $MAIN_JAR:$DEPENDS $MAIN_CLASS
}


compile

retval=$?
if [[ $retval -eq 0 ]]; then
    package
    retval=$?
    if [[ $retval -eq 0 ]]; then
        run_local
    fi
fi
