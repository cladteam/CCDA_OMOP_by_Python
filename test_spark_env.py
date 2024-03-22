#!/usr/bin/env python3

import pyspark.sql.functions as sf
from pyspark.context import SparkContext 
#import pyspark.errors.exceptions.PySparkException
#import pyspark.errors.exceptions.base.SparkRuntimeException

import spark_env
#sc = SparkContext('local', 'test')
#sc.setLogLevel("INFO")

spark_env_object = spark_env.SparkEnv()
spark = spark_env_object.get_spark()

if True:
    try:
        spark_env_object.start()
    #except SparkRuntimeException as sre:
    except Exception as sre:
        print("--ERROR:Spark might already be setup, trying to just run", sre)
        spark_env_object.restart()
else:
    spark_env_object.restart()

print(f"--CURRENT SCHEMA: ")
spark.range(1).select(sf.current_schema()).show()

print("==========")
sql = f"SELECT * from concept where concept_id = 3004249"
print(sql)
result_df = spark.sql(sql)
print(f"num df rows:", result_df.count())
result_df.show()

print("==========")
sql = f"SELECT count(*) from concept"
print(sql)
result_df = spark.sql(sql)
print(f"num df rows:", result_df.count())
result_df.show()

print("==========")
sql = f"SELECT * from concept limit 10"
print(sql)
result_df = spark.sql(sql)
print(f"num df rows:", result_df.count())
result_df.show()


