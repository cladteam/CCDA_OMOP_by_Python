#!/usr/bin/env python3

import pyspark.sql.functions as sf
from pyspark.context import SparkContext 
#import pyspark.errors.exceptions.PySparkException
#import pyspark.errors.exceptions.base.SparkRuntimeException

import spark_util
#sc = SparkContext('local', 'test')
#sc.setLogLevel("INFO")

spark_util_object = spark_util.SparkUtil()
spark = spark_util_object.get_spark()


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


