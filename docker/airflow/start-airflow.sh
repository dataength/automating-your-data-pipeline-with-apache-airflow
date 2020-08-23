#!/usr/bin/env bash

# Create the user airflow in the HDFS
hdfs dfs -mkdir -p    /user/airflow/
hdfs dfs -chmod g+w   /user/airflow

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Initiliase the metadatabase
airflow initdb

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web sever in foreground (for docker logs)
exec airflow webserver