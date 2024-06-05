#!/usr/bin/env python3

""" the beginning of some tests. Needs to evolve. """

import pyspark.sql.functions as sf
# from pyspark.context import SparkContext
# import pyspark.errors.exceptions.PySparkException
# import pyspark.errors.exceptions.base.SparkRuntimeException

from util import spark_util
# sc = SparkContext('local', 'test')
# sc.setLogLevel("INFO")

spark_util_object = spark_util.SparkUtil()
spark = spark_util_object.get_spark()


print("--CURRENT SCHEMA: ")
spark.range(1).select(sf.current_schema()).show()

print("========== describe concept")
SQL = "describe concept"
result_df = spark.sql(SQL)
result_df.show()


print("========== select concept")
SQL = "SELECT * from concept where concept_id = 3004249"
result_df = spark.sql(SQL)
print("num df rows:", result_df.count())
result_df.show()

print("========== select person")
SQL = "SELECT * from person "
print(SQL)
result_df = spark.sql(SQL)
print("num df rows:", result_df.columns, result_df.count())
result_df.show()

print("========== describe person")
SQL = "describe person"
result_df = spark.sql(SQL)
print(result_df)
result_df.show()

print("========== select person (again)")
SQL = "SELECT * from person limit 2"
print(SQL)
result_df = spark.sql(SQL)
print("num df rows:", result_df.columns, result_df.count())
result_df.show()

print("========== insert person")
SQL = """ INSERT into person
          VALUES  (
          1, 2, 2000, 3, 31,
          null, 1, 2, 3
          ,0, 0,
          '',   '', 0,   '', 0,   '', 0
          )
       """
print(SQL)
result_df = spark.sql(SQL)
print("INSERT PERSON ", result_df, result_df.columns, result_df.count())

print("--CURRENT SCHEMA: ")
spark.range(1).select(sf.current_schema()).show()

print("========== select person (again)")
SQL = "SELECT * from person limit 2"
print(SQL)
result_df = spark.sql(SQL)
print("num df rows:", result_df.columns, result_df.count())
result_df.show()
