#!/bin/bash
hadoop fs -rm -f /user/"$USER"/requests/*json \
    /user/"$USER"/results/hsps/*.txt
hadoop fs -mkdir -p /user/"$USER"/requests/
hadoop fs -mkdir -p /user/"$USER"/results/hsps
hadoop fs -ls /user/"$USER"/requests/

for I in $(seq 100); do
    x="test$RANDOM"
    echo "Test is $x"
    TS=$(date +%Y-%m-%dT%H:%M:%S.%N)
    query=$(grep  -h "TA[ACG]" ../tests/queries/* |sort -R | head -1)
    echo -n "{ " > $x.json
    echo -n "\"timestamp_hdfs\":\"$TS\", " >> $x.json
    echo -n "\"RID\":\"$x\", " >> $x.json
    echo -n "\"db\":\"nt\", " >> $x.json
    echo "\"query_seq\": $query }" >> $x.json
    cat $x.json
done
hadoop fs -copyFromLocal -f test*.json /user/"$USER"/requests/
rm -f test*.json

hadoop fs -ls /user/"$USER"/requests/

