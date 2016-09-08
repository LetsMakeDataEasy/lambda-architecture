FROM java:8

MAINTAINER cyjia15@gmail.com

RUN apt-get update && apt-get install -y wget
RUN \
    wget http://apache.website-solution.net/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz -O /tmp/kafka.tgz && \
    tar xfz /tmp/kafka.tgz -C /opt && \
    mv /opt/kafka_2.11-0.10.0.0 /opt/kafka && \
    rm /tmp/kafka.tgz
ENV KAFKA_HOME /opt/kafka
WORKDIR /opt/kafka
ADD start-server.sh /opt/kafka
