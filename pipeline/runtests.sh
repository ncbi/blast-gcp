#!/bin/bash

clear
echo "just running the tests"

[ -f libblastjni.so ] || gsutil cp gs://blast-lib/libblastjni.so .
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:. mvn -q test

