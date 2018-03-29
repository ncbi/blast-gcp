#!/bin/bash


SPARK_HOME="/usr/lib/spark/jars"
DEPENDS="$SPARK_HOME/*:."
MAIN_JAR="sprint2.jar"

# get rid of eventual leftover compiled classes
rm -rf *.class

echo "compiling java-classes"
javac -Xlint:unchecked -cp $DEPENDS -d . src/*.java
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "compiling java-classes failed"
    exit
fi

echo "packing compiled classes into $MAIN_JAR"
jar cf $MAIN_JAR *class
retval=$?
if [[ $retval -ne 0 ]]; then
    echo "packing compiled classes into $MAIN_JAR failed"
    exit
fi

# get rid of compiled classes after packing them
rm -rf *.class

