#!/bin/bash

/opt/hive/bin/schematool -dbType postgres -initSchema

/opt/hive/bin/hive --service metastore