#!/bin/sh
export PYSPARK_PYTHON=$PWD/venv/bin/python
export PYSPARK_DRIVER_PYTHON=$PWD/venv/bin/python
pyspark --master yarn --queue dhillon-group --num-executors 500 --executor-memory 5g --conf spark.ui.port=4069