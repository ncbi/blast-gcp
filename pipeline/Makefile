# Makefile for building and running spark pipeline
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Wed 30 May 2018 12:43:57 PM EDT

SHELL=/bin/bash
.PHONY: run build

SPARK_BLAST_CLASS=gov.nih.nlm.ncbi.blastjni.BLAST_MAIN
SPARK_BLAST_JAR=target/sparkblast-1-jar-with-dependencies.jar
REGION=us-east4

run: ${SPARK_BLAST_JAR} libblastjni.so address.txt
	./run_spark.sh

${SPARK_BLAST_JAR}:
	./make_jar.sh

address.txt:
	sed -i "s/t7/t6-${USER}/;s/results/&-${USER}/" ini.json
	/sbin/ifconfig | awk -F: '/inet addr/ {print $$2}' | grep -v 127.0.0.1 | tr -s ' ' | cut -d ' ' -f 1 > $@
	gsutil -q cp $@ gs://blast-results-${USER}/


###########################################################################
# Targets to run from NCBI hosts

run_rmt: ${SPARK_BLAST_JAR} libblastjni.so ini-tmp.json rmt_address.txt
	gcloud beta dataproc jobs submit spark --cluster blast-dataproc-${USER} \
		--class=${SPARK_BLAST_CLASS} \
		--jars=${SPARK_BLAST_JAR} \
		--region=${REGION} --labels="owner=${USER},project=blast" \
		--files libblastjni.so,ini-tmp.json,dbs.json \
		-- ini-tmp.json

rmt_address.txt:
	gcloud compute instances describe blast-dataproc-${USER}-m | awk '/networkIP/ {print $$2}' > $@
	gsutil -q cp $@ gs://blast-results-${USER}/address.txt

libblastjni.so:
	[ -f $@ ] || gsutil cp gs://blast-test-greg/$@ .

ini-tmp.json:
	sed "s/t7/t6-${USER}/;s/results/&-${USER}/" ini.json > $@

rmt_jobid.txt:
	gcloud -q beta dataproc jobs list --cluster=blast-dataproc-${USER} --region=${REGION} --state-filter=active | grep -v ^JOB | head -1 | awk '{print $$1}' > $@

rmt_kill: rmt_jobid.txt
	@echo Killing `cat rmt_jobid.txt`
	gcloud -q beta dataproc jobs kill --region=${REGION} `cat rmt_jobid.txt`
	rm $^

rmt_log: rmt_jobid.txt
	gcloud dataproc jobs wait `cat rmt_jobid.txt` --region=${REGION}

### # Ganglia.sh comes from https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git 
### setup_sprint_bucket:
### 	gsutil cp ../lib_builder/cluster_initialize.sh gs://sprint7-integration-demo
### 	gsutil cp ~/ganglia.sh gs://sprint7-integration-demo
