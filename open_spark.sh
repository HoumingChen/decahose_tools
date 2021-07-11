#!/bin/sh
export PYSPARK_PYTHON=/sw/dsi/aarch64/centos7/python/3.7.4/bin/python3
export PYSPARK_DRIVER_PYTHON=/sw/dsi/aarch64/centos7/python/3.7.4/bin/python3
pyspark --master yarn --queue dhillon-group --num-executors 500 --executor-memory 5g --conf spark.ui.port=4069