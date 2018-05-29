#!/bin/bash

echo "compiling java-classes"
#clear
mvn -q package
#mvn package | grep -e WARNING -e ERROR -e SUCCESS | \
#    grep -v -e UPLOADER -e NODES -e READER -e SINGLETON -e SE_UTIL -e DATABASE
#mvn -q package
exit

