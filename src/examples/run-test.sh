#!/bin/bash

TEST_CASE=$1
CONFIG=${2}
BEETEST_DIR=${3}
JAR_FILE=../../target/Beetest*-jar-with-dependencies.jar

hadoop jar $JAR_FILE com.spotify.beetest.TestQueryExecutor ${TEST_CASE} ${CONFIG} ${BEETEST_DIR}
