#!/bin/bash

DST="./src/main/java/BLAST_SPARK_VERSION.java"
CMD="\"`TZ=EST5EDT date`\""

echo "package gov.nih.nlm.ncbi.blastjni;" > $DST
echo "public final class BLAST_SPARK_VERSION { public static final String SRC_DATE = $CMD; }" >> $DST

