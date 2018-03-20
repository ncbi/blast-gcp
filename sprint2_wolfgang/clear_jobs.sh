#!/bin/bash

echo "removing all jobs from hadoop-fs"
hadoop fs -rm todo/*
