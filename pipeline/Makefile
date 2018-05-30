# Makefile for building and running spark pipeline
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Wed 30 May 2018 12:43:57 PM EDT

SHELL=/bin/bash
.PHONY: run build

run: build address.txt
	[ -f libblastjni.so ] || ln ../experiment6/libblastjni.so .
	./run_spark.sh

address.txt:
	sed -i "s/t7/&-${USER}/;s/results/&-${USER}/" ini.json
	/sbin/ifconfig | awk -F: '/inet addr/ {print $$2}' | grep -v 127.0.0.1 | tr -s ' ' | cut -d ' ' -f 1 > $@
	gsutil -q cp $@ gs://blast-results-${USER}/


build: 
	./make_jar.sh

