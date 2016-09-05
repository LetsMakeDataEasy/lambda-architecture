# Pull base image
FROM java:8

ENV SCALA_VERSION 2.11.8
ENV SBT_VERSION 0.13.12

ADD sources.list /etc/apt/sources.list
# Install Scala
RUN \
  curl -fsL http://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /root/ && \
  echo >> /root/.bashrc && \
  echo 'export PATH=~/scala-$SCALA_VERSION/bin:$PATH' >> /root/.bashrc

RUN \
    apt-get update && \
    apt-get install -y apt-transport-https

# Install sbt
RUN \
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 && \
    apt-get update && \
    apt-get install -y sbt

RUN \
  mkdir -p /root/.ivy2 && \
  mkdir -p /root/.sbt && \
  mkdir /playground

VOLUME /root/.ivy2
VOLUME /root/.sbt
VOLUME /playground

# Define working directory
WORKDIR /playground
