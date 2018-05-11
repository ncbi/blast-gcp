#!/bin/bash

echo "removing results from hadoop-fs"
rm -rf results/*
mkdir results
hadoop fs -copyToLocal results/* results
