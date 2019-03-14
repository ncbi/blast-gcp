#!/bin/bash

clear
echo "compiling java-classes"
mvn -q verify -DskipTests
