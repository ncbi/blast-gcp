#!/bin/bash
# convert-results-2-asntxt.sh: converts binary ASN.1 to text ASN.1

export PATH=/bin:/usr/bin
SCRIPT_DIR=$(cd "`dirname "$0"`"; pwd)
set -euo pipefail
shopt -s nullglob

command -v asntool > /dev/null || sudo apt install -y ncbi-tools-bin
command -v parallel > /dev/null || sudo apt install -y parallel

parallel \
    asntool -m ${SCRIPT_DIR}/../lib_builder/asn.all -t Seq-annot -d {} -p {}.txt ::: \
    `find . -type f -name "*.asn1"`
