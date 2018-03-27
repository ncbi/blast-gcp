# Makefile for spark daemon
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Fri 23 Mar 2018 01:14:54 PM EDT
# Tested at NCBI only

.PHONY: all clean distclean

MASTER?=yarn
DRIVER_MEMORY?=2G
EXECUTOR_MEMORY?=4G
CLASS?=GCP_BLAST
NUM_EXECUTORS?=12
NUM_CORES?=4
#DB?=/user/camacho/swissprot
#NREPEATS?=3
#RECSPERMAP?=200000
NAME?=${USER}.${CLASS}.$(shell echo ${DB} | awk -F/ '{print $$NF}' ).${DRIVER_MEMORY}.${NUM_EXECUTORS}.${EXECUTOR_MEMORY}.${NUM_CORES}

JAR_VERSION=$(shell grep version pom.xml  | grep -v xml | head -1 | sed 's/<version>//;s,</version>,,' | tr -d ' ')
JAVA_SRC=$(shell find src -type f -name "*.java")
DESC_ID=jar-with-dependencies
#JAR=target/blastjni-$(JAR_VERSION).jar
JAR=sprint2.jar
FAT_JAR=target/blastjni-$(JAR_VERSION)-${DESC_ID}.jar
GCC_LIBPATH=/opt/ncbi/gcc/4.9.3/lib64
JAVA_LOGGING='-Djava.library.path=${GCC_LIBPATH} -Dlog4j.debug=true -Dlog4j.configuration=file://${PWD}/src/test/resources/log4j.properties'
#JAVA_LOGGING='-Dlog4j.debug=true -Dlog4j.configuration=file://${PWD}/src/test/resources/log4j.properties'

#########################################################################
# Targets to run at NCBI
.PHONY: run_at_ncbi
run_at_ncbi: ${JAR} test.ini
	${RM} blast-spark.log
	SPARK_PRINT_LAUNCH_COMMAND=1 time \
    spark-submit --master ${MASTER} --queue prod.blast \
		--name ${NAME} --verbose \
		--conf spark.driver.extraLibraryPath=${GCC_LIBPATH} \
		--conf spark.executor.extraLibraryPath=${GCC_LIBPATH} \
		--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.driver.extraJavaOptions=${JAVA_LOGGING} \
        --conf spark.executor.extraJavaOptions=${JAVA_LOGGING} \
		--driver-memory ${DRIVER_MEMORY} \
		--executor-memory ${EXECUTOR_MEMORY} \
		--num-executors ${NUM_EXECUTORS} \
		--executor-cores ${NUM_CORES} \
		--class ${CLASS} $^

${JAR}: ${JAVA_SRC} Makefile
	javac -Xlint:unchecked -cp ${SPARK_HOME}/jars/*:. -d . ${JAVA_SRC}
	#jar cf $@ `find . -name "*.class"` libblastjni.so
	jar cf $@ `find . -name "*.class"`

#### # Only needed if HDFS is used
#### .PHONY: init_hdfs
#### init_hdfs:
#### 	-hadoop fs -mkdir hdfs:///user/$$USER/todo
#### 	-hadoop fs -mkdir hdfs:///user/$$USER/jobstage
#### 	-hadoop fs -mkdir hdfs:///user/$$USER/results
#### 
#### simple-request.jsonl:
#### 	echo '{"RID": "AFEVGBJD014", "Query": "TAGGATAGGAGCAGTAGTGAATCAAAT", "DB": "nr", "Params": "blastn"}' > $@
#### 
#### .PHONY: submit_job
#### submit_job: simple-request.jsonl
#### 	hadoop fs -put $< hdfs:///user/$$USER/jobstage

#########################################################################
# Targets to run at GCP - untested
.PHONY: run_at_gcp
run_at_gcp: ${JAR} test.ini
	./submit-spark-job-to-gcp.sh

FILES2EXPORT=submit-spark-job-to-gcp.sh libblastjni.so Makefile ${JAR} test.ini
BUCKET=gs://$$USER-test
export_to_gcs:
	gsutil -qm cp ${FILES2EXPORT} ${BUCKET}/
import_from_gcs:
	gsutil -qm cp $(addprefix ${BUCKET}/,${FILES2EXPORT}) .

###${JAR}: ${JAVA_SRC}
###	mvn -q package
###
###${FAT_JAR}: ${JAVA_SRC}
###	mvn -q assembly:assembly -DdescriptorId=${DESC_ID}

clean:
	find . -type f \( -name "*.jar" -o -name "*.class" \) -delete 

distclean: clean
	${RM} -r ${JAR} ${FAT_JAR}
	${RM} cscope.* tags vim.paths

#########################################################################
# Code navigation aids

cscope.files:
	[ -s $@ ] || ack -f --sort-files --cc --cpp --java | xargs realpath > $@

tags: cscope.files
	ctags -L $^

cscope.out: cscope.files
	cscope -bq

vim.paths: cscope.files
	xargs -n1 -I{} dirname {} < $^ | sort -u | sed 's/^/set path+=/' > $@

