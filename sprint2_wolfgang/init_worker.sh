#!/bin/bash

#execute on the worker in the home-directory:
#git clone https://github.com/ncbi/blast-gcp.git
#cd blast-gcp
#git checkout engineering
#cd sprint2_wolfgang
#./init_worker

sudo apt-get install asn1c python-pyasn1 dumpasn1 libtasn1-bin libdw-dev -y

BLASTTMP=/tmp/blast/
BLASTDBDIR=$BLASTTMP/db

get_part()
{
    part=`printf 'nt_50M.%.02d' "$1"`
    mkdir -p $BLASTDBDIR/$part
    cd $BLASTDBDIR/$part
    gsutil -m cp gs://nt_50mb_chunks/$part.*in .
    gsutil -m cp gs://nt_50mb_chunks/$part.*sq .
    touch done
}

#0..886
for nr in {0..30}
do
    get_part $nr
done

sudo chown -R spark:spark $BLASTTMP
sudo chmod -R ugo+rw $BLASTTMP

#MAXJOBS=8
#parts=`gsutil ls gs://nt_50mb_chunks/  | cut -d'.' -f2 | sort -nu`
#for part in $parts; do
#    piece="nt_50M.$part"
#    mkdir -p $BLASTDBDIR/$piece
#    cd $BLASTDBDIR/$piece
#    #mkdir lock
#    gsutil -m cp gs://nt_50mb_chunks/$piece.*in . &
#    gsutil -m cp gs://nt_50mb_chunks/$piece.*sq . &
#    touch done
    #rmdir lock

#    j=`jobs | wc -l`
#    while [ $j -ge $MAXJOBS ]; do
#        j=`jobs | wc -l`
#        echo "$j waiting ..."
#        sleep 0.5
#    done
#done

#sleep 20

echo Cluster Initialized
date
exit 0
