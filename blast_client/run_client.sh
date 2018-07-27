#!/bin/bash

JAVA="/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"
BLAST_CLIENT_CLASS="gov.nih.nlm.ncbi.blast_client.blast_client"
BLAST_CLIENT_JAR="./target/blast_client-1-jar-with-dependencies.jar"
PORT_NR="12345"
REQUEST_LIST="./request_list.txt"
DB_LOCATIONS="./nr_list.txt"

ARGC="$#"
if [ $ARGC -gt 0 ]; then
	PORT_NR="$1"
fi;

echo "opening at port $PORT_NR"
$JAVA -cp $BLAST_CLIENT_JAR $BLAST_CLIENT_CLASS $PORT_NR $REQUEST_LIST $DB_LOCATIONS

