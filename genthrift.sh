#!/bin/sh

rm -rf gen-javabean src/main/java/la/schema
thrift -r --gen java:beans,hashcode,nocamel src/main/resources/schema.thrift
mv gen-javabean/la/schema src/main/java/la/schema
rm -rf gen-javabean
