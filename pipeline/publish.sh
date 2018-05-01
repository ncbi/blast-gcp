PROJECT="ncbi-sandbox-blast"
#TOPIC="spark-test-topic"
TOPIC="input-queue-boratyn"
MESSAGE=`cat $1`

gcloud pubsub topics publish $TOPIC --project $PROJECT --message "$MESSAGE"

