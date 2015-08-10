#!/bin/bash

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <beetest-jar-path> <test-case-folder> <path-to-hive-site.xml>"
  exit 1
fi

JAR_PATH=$1
TEST_CASE=$2
CONFIG=$3
USE_MINI_CLUSTER=TRUE
DELETE_TEST_DIR_ON_EXIT=TRUE

CP=$(find `pwd` $JAR_PATH -name "*.jar" | tr "\n" ":")
java -cp $CP                            \
  -Dhadoop.root.logger=ERROR,console    \
  com.spotify.beetest.TestQueryExecutor \
  ${TEST_CASE} ${CONFIG} ${USE_MINI_CLUSTER} ${DELETE_TEST_DIR_ON_EXIT} \
  2>&1 | grep -v MetricsSystemImpl
