import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]") \
    .config("spark.hadoop.yarn.resourcemanager.principal","jfletcher")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-west-2")\
    .getOrCreate()

schema = StructType(
  [
    StructField("customerID", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("SeniorCitizen", StringType(), True),
    StructField("Partner", StringType(), True),
    StructField("Dependents", StringType(), True),
    StructField("tenure", DoubleType(), True),
    StructField("PhoneService", StringType(), True),
    StructField("MultipleLines", StringType(), True),
    StructField("InternetService", StringType(), True),
    StructField("OnlineSecurity", StringType(), True),
    StructField("OnlineBackup", StringType(), True),
    StructField("DeviceProtection", StringType(), True),
    StructField("TechSupport", StringType(), True),
    StructField("StreamingTV", StringType(), True),
    StructField("StreamingMovies", StringType(), True),
    StructField("Contract", StringType(), True),
    StructField("PaperlessBilling", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("MonthlyCharges", DoubleType(), True),
    StructField("TotalCharges", DoubleType(), True),
    StructField("Churn", StringType(), True)
  ]
)    
    
telco_data = spark.read.csv(
  "s3a://ml-field/demo/telco/",
  header=True,
  schema=schema,
  sep=',',
  nullValue='NA'
)

telco_data.show()

telco_data.printSchema()

telco_data.coalesce(1).write.csv(
  "file:/home/cdsw/raw/telco-data/",
  mode='overwrite',
  header=True
)

spark.sql("show databases").show()

spark.sql("show tables in default").show()

telco_data.write.saveAsTable(
  'default.telco_data',
   format='parquet', 
   mode='overwrite'
)

spark.sql("select * from default.telco_data").show()
spark.sql("show create table default.telco_data").take(1)

#   path='s3a://prod-cdptrialuser19-trycdp-com/cdp-lake/data/airlines/airline_parquet_table')