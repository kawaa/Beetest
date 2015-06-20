#!/bin/bash

TEST_CASE=$1
CONFIG=${2:-local-config}
USE_MINI_CLUSTER=$3
DELETE_TEST_DIR_ON_EXIT=$4

java -cp $(for i in ../../target/jars/*.jar ; do echo -n $i: ; done) com.spotify.beetest.TestQueryExecutor ${TEST_CASE} ${CONFIG} ${USE_MINI_CLUSTER} ${DELETE_TEST_DIR_ON_EXIT}
