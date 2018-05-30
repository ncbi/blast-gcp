#FROM debian:8 # What DataProc runs under
#FROM centos:7 # What NCBI runs under
# gcp-deployer is CentOS 7 based
FROM us.gcr.io/ncbi-sandbox-blast/gcp-deployer:0.1

##################################
# Below needed for building JARs, not running them on Dataproc:
# If Debian8:
#RUN echo "deb http://http.debian.net/debian jessie-backports main" | tee --append /etc/apt/sources.list.d/jessie-backports.list > /dev/null
#RUN apt-get update -y && apt-get -y install
#RUN apt-get install -y -t jessie-backports openjdk-8-jdk
#RUN update-java-alternatives -s java-1.8.0-openjdk-amd64
#RUN apt-get install -y maven
#
# if CentOS 7:
#RUN yum install -y maven google-cloud-sdk java-1.8.0-openjdk
#
#RUN mvn -q package
##################################

#RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
#    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
#    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
#    apt-get update -y && apt-get install google-cloud-sdk -y

COPY ./ /app
#COPY libblastjni.so libblastjni.so
#COPY sparkblast-1-jar-with-dependencies.jar \
#        sparkblast-1-jar-with-dependencies.jar

