clear
CMD="mvn -q test -Dtest=Test_Multi_JVM_Locking.java"
echo $CMD
eval $CMD

