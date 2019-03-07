#!/bin/bash

renice +19 -p $$

MAXJOBS=2

pushd "$TMP" || exit
gsutil -m cp -n gs://blast-test-requests-sprint11/*json .
popd || exit

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:."
TESTS=$(/bin/ls -1 $TMP/A*.json)
for test in $TESTS; do
    if [ ! -f "$test.fix" ]; then
        ~/blast-gcp/lib_builder/fixjson.py "$test" &
        sleep 0.01
    fi
done
wait

#FIXED=$(/bin/ls -1 $TMP/*fix)

rm -f blast_worker.log

for PARTITION in $(seq 1 800); do
    echo "Starting blast_worker for partition $PARTITION"
    SAMPLE=$(/bin/ls -1 $TMP/*fix | sort -R | head -n 100)
    valgrind -v "--log-file=blast_worker.vg.$RANDOM" \
        --leak-check=full --show-leak-kinds=all \
        ./blast_worker 1 "$PARTITION" $SAMPLE >> "$TMP/blast_worker.log" 2>&1 &
    j=$(jobs | wc -l)
    while [ "$j" -ge "$MAXJOBS" ]; do
        j=$(jobs | wc -l)
        sleep 0.5
    done

done


wait
date

