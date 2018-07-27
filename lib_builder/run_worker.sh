#!/bin/bash

renice +6 -p $$

export LD_LIBRARY_PATH="$LD_LIBARAY_PATH:."
TESTS=$(/bin/ls -1 $TMP/A*.json | head )
for test in $TESTS; do
    if [ ! -f "$test.fix" ]; then
        ~/blast-gcp/experiment_dataset2/fixjson.py "$test" &
        sleep 0.01
    fi
done
wait

FIXED=$(/bin/ls -1 $TMP/*fix)

./blast_worker 16 10 $FIXED > blast_worker.log 2>&1

valgrind -v --log-file=vg --leak-check=full \
./blast_worker 16 10 $FIXED > blast_worker_vg.log 2>&1
