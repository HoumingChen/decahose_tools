#!/bin/sh
virtualenv --python=/sw/dsi/aarch64/centos7/python/3.7.4/bin/python3 venv
venv/bin/python -m pip install -r requirements.txt
export PYSPARK_PYTHON=/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/venv/bin/python
pyspark --master yarn --queue dhillon-group --num-executors 500 --executor-memory 5g --conf spark.ui.port=4069