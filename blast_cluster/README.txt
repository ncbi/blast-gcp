How to build the application:
	> run the script './build.sh'

How to generate javadoc:
	> run the script './doc.sh'
	( the index.html file will be generated in ./target/site/apidocs )

How to run the cluster:
	> check the file 'ini.json' for correct settings
	> run the script './run.sh'
	( the application will be ready to take commands after you see the string 'ready' on the console )

Available commands on the console:

	'exit'	... terminates the application

	'F request-file' ... processes this one request, full url to json-file in bucket
		example: 'F gs://blast-test-requests-sprint6/AFEVGV0P014.json'

	'L request-list' ... processes the list of requests found in the given file
		example: 'L list.txt'

		example-of list-file:
			#this is a comment
			:src=gs://blast-test-requests-sprint6
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
		example: 'B gs://blast-test-requests-sprint6'

	'B bucket-url N' ... processes the first N requests found in the given bucket
		example: 'B gs://blast-test-requests-sprint6 10'

	'stop'	... cancels all running lists ( 'L' and 'B' ) commands
		but tries to finish the jobs which are currently running

where are the results?
	in the directory './report'
