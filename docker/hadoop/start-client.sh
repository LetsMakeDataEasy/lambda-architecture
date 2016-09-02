#!/bin/sh
# A Script to start a hadoop client with a external data directory,
# so we can add file to hdfs and test some map-reduce function

if [ "$1" == "" ]; then
    echo "Usage: ./start-client.sh dir-to-your-data"
    exit 1
fi

docker run -it --link 260_namenode_1:hdfs-namenode --rm --network 260_default -v $1:/data cyjia/hadoop bash
