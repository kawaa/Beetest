#!/bin/bash

TEST_CASE=$1
CONFIG=${2:-config}
JAR_FILE=../../..//target/Beetest-*.jar

hadoop jar $JAR_FILE com.spotify.beetest.TestQueryExecutor ${TEST_CASE} ${CONFIG}
