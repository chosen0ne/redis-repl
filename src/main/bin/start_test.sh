#!/bin/sh

PROJECT_BIN="${BASH_SOURCE-$0}"
PROJECT_BIN="$(dirname "${PROJECT_BIN}")"
PROJECT_DIR="$(cd "${PROJECT_BIN}"/..; pwd)"

CONF_DIR=${PROJECT_DIR}/conf
LIB_DIR=${PROJECT_DIR}/lib
LOG_DIR=${PROJECT_DIR}/logs
mkdir -p $LOG_DIR

MainClass="com.chosen0ne.redisrepl.test.Main"
large="-DtraceEventOpen=true -Xms16000m -Xmx16000m -Xmn14000m -XX:PermSize=256m -XX:MaxTenuringThreshold=15 \
      -XX:+CMSScavengeBeforeRemark -XX:CMSInitiatingOccupancyFraction=80 -XX:SurvivorRatio=30 \
      -XX:ParallelGCThreads=16 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+HeapDumpOnOutOfMemoryError \
      -verbose:gc -XX:+PrintGCDateStamps  -XX:+PrintGCDetails -Xloggc:${LOG_DIR}/gc.log"
exec java -cp $CONF_DIR:$LIB_DIR/* $large -Drun_mode=test -Dconfig_dir=$CONF_DIR -Dlog_dir=$LOG_DIR $MainClass $@