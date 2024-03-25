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

print("========== describe concept")
sql = "describe concept"
result_df = spark.sql(sql)
result_df.show()


print("========== select concept")
sql = f"SELECT * from concept where concept_id = 3004249"
result_df = spark.sql(sql)
print(f"num df rows:", result_df.count())
result_df.show()

print("========== select person")
sql = f"SELECT * from person "
print(sql)
result_df = spark.sql(sql)
print(f"num df rows:", result_df.columns, result_df.count())
result_df.show()

print("========== describe person")
sql = "describe person"
result_df = spark.sql(sql)
print(result_df)
result_df.show()

print("========== select person (again)")
sql = f"SELECT * from person limit 2"
print(sql)
result_df = spark.sql(sql)
print(f"num df rows:", result_df.columns, result_df.count())
result_df.show()

print("========== insert person")
sql = """ INSERT into person
          VALUES  (
          1, 2, 2000, 3, 31,
          null, 1, 2, 3
          ,0, 0, 
          '',   '', 0,   '', 0,   '', 0
          )
       """
          #,null, null, 
          #null,   null, null,   null, null,   null, null
print(sql)
result_df = spark.sql(sql)
print("INSERT PERSON ", result_df, result_df.columns, result_df.count())

print(f"--CURRENT SCHEMA: ")
spark.range(1).select(sf.current_schema()).show()

print("========== select person (again)")
sql = f"SELECT * from person limit 2"
print(sql)
result_df = spark.sql(sql)
print(f"num df rows:", result_df.columns, result_df.count())
result_df.show()


