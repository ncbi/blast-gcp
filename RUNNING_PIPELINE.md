# Starting a cluster

From your google cloud shell (the >_) box in upper right, paste
(ctrl-shift-v in my browser, very finicky)
```shell
# Copied from lib_builder/make_cluster.sh
gcloud beta dataproc --region us-east4 \
    clusters create blast-dataproc-$USER \
    --master-machine-type n1-standard-8 \
        --master-boot-disk-size 100 \
    --num-workers 2 \
        --worker-boot-disk-size 250 \
    --worker-machine-type n1-highcpu-64 \
    --num-preemptible-workers 4 \
        --preemptible-worker-boot-disk-size 250 \
    --scopes cloud-platform \
    --project ncbi-sandbox-blast \
    --labels owner=$USER \
    --region us-east4 \
    --zone   us-east4-b \
    --max-age=8h \
    --image-version 1.2 \
    --initialization-action-timeout 30m \
    --initialization-actions \
    "$PIPELINEBUCKET/scripts/cluster_initialize.sh" \
    --tags blast-dataproc-${USER}-$(date +%Y%m%d-%H%M%S) \
    --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4
```

* Once cluster has begun, click on [ DataProc ->  Clusters ](https://console.cloud.google.com/dataproc/clusters?project=ncbi-sandbox-blast)
* you should see your cluster "Provisioning" (takes about 10 minutes to copy Blast databases from Google Cloud Storage), and then "Running."
* **DATA LOSS WARNING:** Cluster filesystems, including /home and HDFS, are not persistent, and clusters automatically terminate after 8 hours.

# SSH'ing to your cluster
Click on the cluster name, and then "VM instances", and then click on your
master node and open an SSH session on it. If using another ssh client, note that you want the "External IP", marked "(ephemeral)."

We highly recommend creating 4 ssh sessions with an external client into the master node, and arranging them so that all 4 can be viewed on your monitor at the same time. If you have not yet created/registered a key pair with GCP, you will need to do this before connecting with an external client.

# Window 1 - Checkout git repository
```shell
git clone https://github.com/ncbi/blast-gcp.git
cd blast-gcp
git checkout engineering
```

## Compiling and Starting Spark Application
```shell
cd ~/blast-gcp/pipeline
./make_jar.sh
cp ~/blast-gcp/lib_builder/libblastjni.so ~/blast-gcp/pipeline # Avoid if libblastjni.so is in flux
hadoop fs -mkdir -p /user/$USER/requests
cd ~/blast-gcp/pipeline;./run_spark.sh # to start Spark
```
If Blast databases aren't prefetched into /tmp/blast/db, first query may require 5-10 minutes.
### If using a non-standard cluster size:
1. num_db_partitions may be increased to 886.
2. num_executors should be approximately the number of vCPUS on all of your worker nodes, minus some overhead
3. num_executor_cores should be 1, we believe YARN enforces this


# Window 2 - Spark logs
```console
$ nc -lk 10011 &
```

# Window 3 - Test Harness
**PubSub Topic, Subscription and Cloud Storage output bucket will be deleted upon completion**

You'll likely need a ~/google-service-account-file.json
( gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS )

```console
$ cd ~/blast-gcp/tests
$ virtualenv --python python3 env
$ source env/bin/activate
$ cd env
$ pip3 install google-cloud
$ pip3 install google-cloud-pubsub
$ pip3 install google-cloud-storage
$ cd ..
$ ./test-pipeline.py
 . . . 
           *** Start Spark Streaming Job now, press Enter when readu ***
```

# Window 4 - Google Connector
```console
cd ~/blast-gcp/pipeline
virtualenv --python python3 env
source env/bin/activate
cd env
pip3 install google-cloud
pip3 install google-cloud-pubsub
pip3 install google-cloud-storage
~/blast-gcp/pipeline/google_connector blast-test-$USER /user/$USER/requests
```

# Shutdown your cluster
**All data not stored in a Google Cloud Storage bucket will be lost.**
**DATA LOSS WARNING: Cluster filesystems, including /home and HDFS, are not persistent, and clusters automatically terminate after 8 hours. **

```console
In the spark (application terminal : exit
gcloud dataproc --region us-east4 clusters list
gcloud dataproc --region us-east4 clusters delete cluster-$USER-...
```
or [ select your cluster ](https://console.cloud.google.com/dataproc/clusters?project=ncbi-sandbox-blast) and press Delete.
