FROM eclipse-temurin:21-jre-jammy

RUN apt-get update && apt-get install -y unzip

COPY jasdb_assembly/target/*.zip /
RUN unzip jasdb*.zip
RUN rm *.zip
RUN mv jasdb* /jasdb
RUN chmod +x /jasdb/*.sh

ARG JAR_FILE=target/command-svc*.jar
ENV JASDB_HOME /jasdb-data
ENV JAVA_HOME=/opt/java/openjdk

WORKDIR '/jasdb'
CMD bash -C '/jasdb/start.sh'

VOLUME /jasdb-data

EXPOSE 7050
MAINTAINER Renze de Vries