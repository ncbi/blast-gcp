#!/bin/bash -x

LOCAL_MAVEN_REPO=$(realpath $(cd "`dirname "$0"`"; pwd)/.local-maven-repo)
NCBI_BLAST_PACKAGE=gov.nih.nlm.ncbi.blast
STATUS_TRACKER_VER=0.3.12
STATUS_TRACKER_ARTF_ID=blast-gcp-status-tracker
STATUS_TRACKER_JAR=${STATUS_TRACKER_ARTF_ID}-${STATUS_TRACKER_VER}.jar
NCBI_BUILD_ARTIFACTS=gs://ncbi-build-artifacts

if [ ! -d $LOCAL_MAVEN_REPO ] ; then 
    mkdir -p $LOCAL_MAVEN_REPO
    gsutil -qm cp ${NCBI_BUILD_ARTIFACTS}/${STATUS_TRACKER_JAR} .
    mvn install:install-file \
        -Dfile=${STATUS_TRACKER_JAR} \
        -DgroupId=${NCBI_BLAST_PACKAGE} \
        -DartifactId=${STATUS_TRACKER_ARTF_ID} \
        -Dversion=${STATUS_TRACKER_VER} \
        -Dpackaging=jar \
        -DgeneratePom=true \
        -DlocalRepositoryPath=${LOCAL_MAVEN_REPO}
    find . -name "*.jar"
fi 

mvn package

# This is finicky at GCP
if [ $? -ne 0 ] ; then
    mvn install:install-file \
        -Dfile=${STATUS_TRACKER_JAR} \
        -DgroupId=${NCBI_BLAST_PACKAGE} \
        -DartifactId=${STATUS_TRACKER_ARTF_ID} \
        -Dversion=${STATUS_TRACKER_VER} \
        -Dpackaging=jar \
        -DgeneratePom=true
    mvn package
fi

