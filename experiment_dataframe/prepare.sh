#!/bin/bash

BLASTTMP=/tmp/blast/
BLASTDBDIR=$BLASTTMP/db

get_part()
{
    part=`printf 'nt_50M.%.02d' "$1"`
    mkdir -p $BLASTDBDIR/$part
    cd $BLASTDBDIR/$part
#    gsutil -m cp gs://nt_50mb_chunks/$part.*in .
#    gsutil -m cp gs://nt_50mb_chunks/$part.*sq .
    gsutil -m cp gs://nt_50mb_chunks/$part.*hr .
}

#0..886
for nr in {0..60}
do
    get_part $nr
done

sudo chown -R spark:spark $BLASTTMP
sudo chmod -R ugo+rw $BLASTTMP
