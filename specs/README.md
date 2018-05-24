# BLAST Request Spec

### Generating sources

`
virtualenv -p python3 build-env
source build-env/bin/activate
pip install -r requirements.txt
make`

### Converting request from text format to binary

cat samples/blreq_sample.2 | protoc --encode=ncbi.blast.blast_request.BlastRequest blast_request.proto | od -tc

See samples subdirectory for requests in text format.

