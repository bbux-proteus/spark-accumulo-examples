#!/bin/bash

stty -echo
read -p "Password: " passw; echo
stty echo

java -cp target/spark-accumulo-ingest-0.1.0-SNAPSHOT-shaded.jar org.bbux.spark.AccumuloTest \
  local \
  root \
  $passw \
  grades \
  pixydust \
  localhost
