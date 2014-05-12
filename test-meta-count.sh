#!/bin/bash

if [ $# -lt 1 ] ; then
   echo "Please specify number of iterations to run!" >&2; exit 1
fi

stty -echo
#read -p "Password: " passw; echo
stty echo

jar='target/spark-accumulo-ingest-*-SNAPSHOT-shaded.jar'

java -cp ${jar} org.bbux.spark.AccumuloMetadataCount \
  local \
  root \
  s3cret \
  malware_domain_list \
  pixydust \
  localhost \
  $1
