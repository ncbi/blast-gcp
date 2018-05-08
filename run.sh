#!/bin/bash
# run.sh: Runs spark daemon
#
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Tue 08 May 2018 06:42:38 PM EDT

export PATH=/bin:/usr/bin
set -euo pipefail
shopt -s nullglob

git checkout fix-blastdb-files-to-download && \
    yes | sudo apt-get install maven && \
    cd pipeline && ./make_jar.sh && \
    ln -s ../lib_builder/*.so . && \
    ./run_spark.sh
