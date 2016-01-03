FROM java:8
COPY jasdb_assembly/target/*.zip /
RUN unzip jasdb*.zip
RUN rm *.zip
RUN mv jasdb* /jasdb
RUN chmod +x /jasdb/*.sh

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV JASDB_HOME /jasdb-data

WORKDIR '/jasdb'
CMD bash -C '/jasdb/start.sh'

VOLUME /jasdb-data

EXPOSE 7050
MAINTAINER Renze de Vries