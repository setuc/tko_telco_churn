from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]") \
    .config("spark.hadoop.yarn.resourcemanager.principal","jfletcher")\
    .getOrCreate()

telco_data = spark.read.csv("s3a://ml-field/demo/telco/",header=True)