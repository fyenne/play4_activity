#!/bin/bash
# pyspark python3 py2 
export PYSPARK_PYTHON="/app/anaconda3/bin/python"
export PYSPARK_DRIVER_PYTHON="/app/anaconda3/bin/python"
spark-submit --master yarn --deploy-mode client --driver-memory 8G --executor-memory 8G --queue root.dsc ./activity_script.py --tables  ${tables} --inc_day ${inc_day} 