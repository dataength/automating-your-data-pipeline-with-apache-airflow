#!/bin/bash

. "$SPARK_HOME/sbin/spark-config.sh"
. "$SPARK_HOME/bin/load-spark-env.sh"

mkdir -p $SPARK_WORKER_LOG_DIR

ln -sf /dev/stdout $SPARK_WORKER_LOG_DIR/spark-worker.out

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port $SPARK_WORKER_UI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG_DIR/spark-worker.out