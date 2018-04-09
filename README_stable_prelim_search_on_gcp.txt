##########################################################
    steps to make this branch work:
##########################################################

on gcp: make shure the worker nodes have the databases in this layout:

/tmp/blast/db/nt_50M.00:
-rw-rw-rw- 1 spark spark        0 Mar 29 20:09 done
-rw-rw-rw- 1 spark spark  1983068 Mar 29 20:09 nt_50M.00.nin
-rw-rw-rw- 1 spark spark 49999925 Mar 29 20:09 nt_50M.00.nsq

/tmp/blast/db/nt_50M.01:
-rw-rw-rw- 1 spark spark        0 Mar 29 20:09 done
-rw-rw-rw- 1 spark spark  1154360 Mar 29 20:09 nt_50M.01.nin
-rw-rw-rw- 1 spark spark 49999868 Mar 29 20:09 nt_50M.01.nsq

and so on.....

on gcp: make shure they are owned by the 'spark'-user

Use the script '/blast-gcp/sprint2_wolfgang/init_worker.sh' to put them into place.
Edit the script to get more sections.

Make shure you do not ask for more db-sections via ini-file as you have in the filesystem present.
Request for a none-existing db-section/partition/chunk ( however you name it ) will cause a segfault!
To be fixed later.

Make shure you have the databases on the master-node too, if you want to test locally.

Because of the size-limit of 100 MB for git-files:

Copy and uncompress the library:
--------------------------------
The library is in 'blast-gcp/libblast_archive/2018_03_30_eugene/libblastjni.so.gz'
It has to be decompressed via 'gunzip' and copied to 'blast-gcp/sprint2_wolfgang/'.
The lib in this location has to be overwritten.

Do not add/commit the uncompressed lib. Github will reject your 'git push'!
This will damage your local git-repo!

Starting the streaming-job on the cluster:
------------------------------------------

Edit the file 'test.ini' to adjust the settings.

In another terminal: 'ncat -lk 10011' to see the log-output
In another terminal: 'ncat -lk 10012' to trigger jobs

( 'sudo apt-get install nmap' to get ncat )

In 'blast-gcp/sprint2_wolfgang' run these 2 scripts:
./make_jar.sh
./run_spark.sh

Edit the script run_spark.sh to adjust local/yarn, number of executors/cores

on google-cluster:
  --num-executers X   : X should match the number or worker-nodes
  --executor-cores Y  : Y should match the number of vCPU's per worker-node 

