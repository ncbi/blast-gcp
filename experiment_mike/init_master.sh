#!/bin/bash

sudo apt-get install asn1c python-pyasn1 dumpasn1 libtasn1-bin libdw-dev netcat -y

git clone https://github.com/ncbi/blast-gcp.git
cd blast-gcp
git checkout engineering
cd sprint2_wolfgang
./init_worker

