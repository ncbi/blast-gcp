#!/bin/bash

echo "removing results from hadoop-fs"
rm ./results/*
hadoop fs -copyToLocal results/* ./results
