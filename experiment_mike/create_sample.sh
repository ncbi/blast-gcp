TS=`date +%Y-%m-%dT%H:%M:%S.%N`
echo ' { "timestamp": "'$TS'", "RID": "sample", "db": "nt", "query_seq": "ATAGGAAGTTATATTAAGGGTTCCGGATCTGGATC" }' > sample.json
cat sample.json
hadoop fs -copyFromLocal -f sample.json /user/vartanianmh/sample.json

#hadoop fs -rm -f -R /user/vartanianmh/requests/
#hadoop fs -rmdir /user/vartanianmh/requests/
hadoop fs -mkdir -p /user/vartanianmh/requests/

for x in "test1" "test2" "test3" "test4" "test5"; do
    TS=`date +%Y-%m-%dT%H:%M:%S.%N`
    echo ' { "timestamp" : "'$TS'", "RID":  "'$x'", "db": "nt", "query_seq": "ATAGGAAGTTATATTAAGGGTTCCGGATCTGGATC" } ' > test$x.json
    cat test$x.json
    hadoop fs -copyFromLocal -f test$x.json /user/vartanianmh/requests/
    rm -f test$x.json
done

hadoop fs -ls /user/vartanianmh/requests/

rm -f parts.json
for x in $(seq 1 886); do
    num=`printf "%02d" $x`
    part=`printf "nt_50M.%02d" $x`
    echo '{ "db" : "nt", "num" : "' $num '", "part": "' $part '" }' >> parts.json
done

hadoop fs -copyFromLocal -f parts.json /user/vartanianmh/parts.json
