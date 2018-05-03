#!/bin/bash

echo "removing results from hadoop-fs"
hadoop fs -rm -r -f results/*
rm -rf results
