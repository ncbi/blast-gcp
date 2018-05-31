# Makefile for building and running spark pipeline
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Wed 30 May 2018 12:43:57 PM EDT

SHELL=/bin/bash
.PHONY: run build

SPARK_BLAST_CLASS=gov.nih.nlm.ncbi.blastjni.BLAST_MAIN
SPARK_BLAST_JAR=target/sparkblast-1-jar-with-dependencies.jar

run: ${SPARK_BLAST_JAR} libblastjni.so address.txt
	./run_spark.sh

${SPARK_BLAST_JAR}:
	./make_jar.sh

address.txt:
	sed -i "s/t7/t6-${USER}/;s/results/&-${USER}/" ini.json
	/sbin/ifconfig | awk -F: '/inet addr/ {print $$2}' | grep -v 127.0.0.1 | tr -s ' ' | cut -d ' ' -f 1 > $@
	gsutil -q cp $@ gs://blast-results-${USER}/

run_rmt: ${SPARK_BLAST_JAR} libblastjni.so ini-tmp.json
	gcloud beta dataproc jobs submit spark --cluster blast-dataproc-${USER} \
		--class=${SPARK_BLAST_CLASS} \
		--jars=${SPARK_BLAST_JAR} \
		--region=us-east4 --labels="owner=${USER},project=blast" \
		--files libblastjni.so,ini-tmp.json \
		-- ini-tmp.json

libblastjni.so:
	[ -f $@ ] || gsutil cp gs://blast-test-greg/$@ .

ini-tmp.json:
	sed "s/t7/t6-${USER}/;s/results/&-${USER}/" ini.json > $@

### # Ganglia.sh comes from https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git 
### setup_sprint_bucket:
### 	gsutil cp ../lib_builder/cluster_initialize.sh gs://sprint7-integration-demo
### 	gsutil cp ~/ganglia.sh gs://sprint7-integration-demo
