#!/bin/bash

echo "Check pom.xml for vulnerable libraries"
mvn org.owasp:dependency-check-maven:1.4.0:aggregate

