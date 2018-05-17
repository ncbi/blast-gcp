FROM debian:8
#FROM gcr.io/ncbi-prodnot/gcp-deployer:gcp-deployer-0.1

#RUN echo "deb http://http.debian.net/debian jessie-backports main" | tee --append /etc/apt/sources.list.d/jessie-backports.list > /dev/null

#RUN apt-get update -y && apt-get -y install

#RUN apt-get install -y -t jessie-backports openjdk-8-jdk

#RUN update-java-alternatives -s java-1.8.0-openjdk-amd64

#RUN apt-get install -y maven

COPY ./ ./

#RUN mvn -q package

#ENTRYPOINT [ " " ]
