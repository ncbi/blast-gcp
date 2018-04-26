# Makefile for sprint4 demo
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Wed 25 Apr 2018 01:26:16 PM EDT

.PHONY: all clean distclean check

SUBSCRIPTION?=sprint4-integration-demo
DEMO_BUCKET=sprint4-integration-demo

all: 
	-gsutil mb gs://${DEMO_BUCKET}
	gsutil cp ./lib_builder/cluster_initialize.sh gs://${DEMO_BUCKET}/scripts/
	#pipeline/make_jar.sh
	#gcloud pubsub subscriptions create --topic run-queue-integration ${SUBSCRIPTION}
	lib_builder/make_cluster.sh gs://${DEMO_BUCKET}


clean:
	gcloud pubsub subscriptions delete ${SUBSCRIPTION}


#########################################################################
# Code navigation aids

cscope.files:
	[ -s $@ ] || ack -f --sort-files --cc --cpp --java | sed "s,^,${PWD}/," > $@

tags: cscope.files
	ctags `cat $^`

cscope.out: cscope.files
	cscope -bq

vim.paths: cscope.files
	xargs -n1 -I{} dirname {} < $^ | sort -u | sed 's/^/set path+=/' > $@

