PROJECT="ncbi-sandbox-blast"
#TOPIC="spark-test-topic"
TOPIC="spark-test-topic"
MESSAGE=`cat $1`

gcloud pubsub topics publish $TOPIC --project $PROJECT --message "$MESSAGE"

