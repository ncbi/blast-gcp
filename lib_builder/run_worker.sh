#!/bin/bash

renice +6 -p $$

MAXJOBS=8

pushd $TMP
gsutil -m cp -n gs://blast-test-requests-sprint6/*json .
popd

export LD_LIBRARY_PATH="$LD_LIBARAY_PATH:."
TESTS=$(/bin/ls -1 $TMP/A*.json)
for test in $TESTS; do
    if [ ! -f "$test.fix" ]; then
        ~/blast-gcp/experiment_dataset2/fixjson.py "$test" &
        sleep 0.01
    fi
done
wait

FIXED=$(/bin/ls -1 $TMP/*fix)

for PARTITION in $(seq 1 800); do
    ./blast_worker 16 $PARTITION $FIXED > blast_worker.log 2>&1 &
    j=`jobs | wc -l`
    while [ $j -ge $MAXJOBS ]; do
        j=`jobs | wc -l`
        sleep 0.5
    done

done

#valgrind -v --log-file=blast_worker.vg --leak-check=full ./blast_worker 16 800 $FIXED > blast_worker_vg.log 2>&1 &

wait
date

