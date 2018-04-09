#!/bin/bash

echo "removing results from hadoop-fs"
hadoop fs -copyToLocal results/* ./results
