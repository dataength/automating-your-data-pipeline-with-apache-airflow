#!/bin/bash

hdfs dfs -mkdir       /tmp
hdfs dfs -mkdir -p    /user/hive/warehouse
hdfs dfs -chmod g+w   /tmp
hdfs dfs -chmod g+w   /user/hive/warehouse

cd ${HIVE_HOME}/bin
./hiveserver2 --hiveconf hive.server2.enable.doAs=false --hiveconf hive.root.logger=DEBUG,console