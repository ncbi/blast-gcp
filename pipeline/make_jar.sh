#!/bin/bash

clear
echo "compiling java-classes"
./blast_spark_version.sh
mvn -q package

