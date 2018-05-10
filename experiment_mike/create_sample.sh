#TS=`date +%Y-%m-%dT%H:%M:%S.%N`
#echo ' { "timestamp": "'$TS'", "RID": "sample", "db": "nt", "query_seq": "ATAGGAAGTTATATTAAGGGTTCCGGATCTGGATC" }' > sample.json
#cat sample.json
#hadoop fs -copyFromLocal -f sample.json /user/vartanianmh/sample.json

#hadoop fs -rm -f -R /user/vartanianmh/requests/
#hadoop fs -rmdir /user/vartanianmh/requests/
hadoop fs -mkdir -p /user/vartanianmh/requests/
hadoop fs -ls /user/vartanianmh/requests/

for I in `seq 100`; do
    x="test$RANDOM"
    echo "Test is $x"
    TS=`date +%Y-%m-%dT%H:%M:%S.%N`
    echo ' { "timestamp_hdfs" : "'$TS'", "RID":  "'$x'", "db": "nt", "query_seq": "ATAGGAAGTTATATTAAGGGTTCCGGATCTGGATC" } ' > $x.json
    cat $x.json
done
hadoop fs -copyFromLocal -f test*.json /user/vartanianmh/requests/
rm -f test*.json

hadoop fs -ls /user/vartanianmh/requests/

#rm -f parts.json
#for x in $(seq 1 886); do
#    num=`printf "%02d" $x`
#    part=`printf "nt_50M.%02d" $x`
#    echo '{ "db" : "nt", "num" : "' $num '", "part": "' $part '" }' >> parts.json
#done
#
#hadoop fs -copyFromLocal -f parts.json /user/vartanianmh/parts.json
