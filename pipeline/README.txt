How to build the application:
	> run the script './build.sh'

How to generate javadoc:
	> run the script './doc.sh'
	( the index.html file will be generated in ./target/site/apidocs )

How to run the cluster:
	> check the file 'ini.json' for correct settings
	> run the script './run.sh'
	( the application will be ready to take commands after you see the string 'ready' on the console )

Notes on the number of partitions spark uses to split the BLASTDB RDD:
This can be configured on the `ini.json` file in the cluster section. If not
set (or set to 0), Spark will split the RDD into the number of partitions of
its choosing, otherwise the provided value is used.

Available commands on the console:

	'exit'	... terminates the application

	'F request-file' ... processes this one request, full url to json-file in bucket
		example: 'F gs://blast-test-requests-sprint11/AFEVGV0P014.json'

	'L request-list' ... processes the list of requests found in the given file
		example: 'L list.txt'

		example-of list-file:
			#this is a comment
			:src=gs://blast-test-requests-sprint11
			AFEVGV0P014.json
			AFEVHK3N014.json
			AFEVJ4EE014.json
			AFEVKCK9014.json
			AFEVKWCH014.json
			AFEVMVZM014.json
			AFEVN6PB014.json
			AFEVP7WY014.json

	'L request-list N' ... processes the first N request in the given list
		example: 'L list.txt 3'

	'B bucket-url' ... processes all requests found in the given bucket
		example: 'B gs://blast-test-requests-sprint11'

	'B bucket-url N' ... processes the first N requests found in the given bucket
		example: 'B gs://blast-test-requests-sprint11 10'

	'stop'	... cancels all running lists ( 'L' and 'B' ) commands
		but tries to finish the jobs which are currently running

    'I' ... info, prints
        - how many entries are in the request-queue
        - how many jobs are active

where are the results?
	in the directory './report'

how to perform a fully scripted test-run:
    create a text-file ( for instance 'b.txt' ) with the following content

        :src=gs://blast-test-requests-sprint11
        :pick 25
        :wait
        :exit
        #pick the first 25 requests from the src-bucket
        #wait for the request-queue and the jobs to finish
        #exit the application
    or
        :src=gs://blast-test-requests-sprint11
        :pick
        :wait
        :exit
        #pick all requests from the src-bucket
        #wait for the request-queue and the jobs to finish
        #exit the application
    or
        :src=gs://blast-test-requests-sprint11
        AFEVGV0P014.json
        AFEVHK3N014.json
        AFEVJ4EE014.json
        AFEVKCK9014.json
        :wait
        :exit
        #execute these specific requests
        #wait for the request-queue and the jobs to finish
        #exit the application

    then run the applictaion with this list on startup
        ./start.sh b.txt

how to set the number of executors?
    It is now possible to not set the number of executors in the ini.json-file.
    Internally the default value will be zero.
    If the number of executors is zero BC_SETTINGS_READER.createSparkConfAndConfigure()
    will enable dynamic alloction of executors.
    Tests have shown that this will fully utilize the cluster after about 5-10 requests.

