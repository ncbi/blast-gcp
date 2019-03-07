#!/bin/bash

renice +19 -p $$

MAXJOBS=2

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/home/vartanianmh/blast-gcp/lib_builder/:."

TESTS=$(/bin/ls -1 $TMP/tests/A*.json | sort -R)
for test in $TESTS; do
    ~/blast-gcp/lib_builder/fixjson.py "$test"
    OUT="$TMP/$(basename $test).out"
    echo "$OUT"
    echo "Running $test -> $OUT"
    echo "$test" >> "$OUT"
    valgrind -v \
        ~/blast-gcp/lib_builder/blast_worker "$test.fix" >> "$OUT" 2>&1 &
    j=`jobs | wc -l`
    while [ $j -ge $MAXJOBS ]; do
        j=`jobs | wc -l`
        sleep 0.5
    done
done
