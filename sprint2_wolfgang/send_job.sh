#!/bin/bash

JOBFILE=job_$1.txt

for arg in "$@"
do
    echo "job_$arg" >> $JOBFILE
done

echo "job_$1" > ./$JOBFILE
hadoop fs -copyFromLocal ./$JOBFILE todo/$JOBFILE
rm ./$JOBFILE

