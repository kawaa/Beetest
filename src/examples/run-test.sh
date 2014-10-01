#!/bin/bash

TEST_CASE=$1
CONFIG=${2:-local-config}
JAR_FILE=../../target/Beetest*-jar-with-dependencies.jar
export HADOOP_CONF_DIR=`dirname ${CONFIG}`

hadoop jar $JAR_FILE com.spotify.beetest.TestQueryExecutor ${TEST_CASE} ${CONFIG}
