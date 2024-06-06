#!/usr/bin/env python3

""" the beginning of some tests. Needs to evolve. """

# from pyspark.context import SparkContext
# import pyspark.errors.exceptions.PySparkException
# import pyspark.errors.exceptions.base.SparkRuntimeException

from util import spark_util
# sc = SparkContext('local', 'test')
# sc.setLogLevel("INFO")

spark_util_object = spark_util.SparkUtil()
spark = spark_util_object.get_spark()

# use a CTE in-line to create something to select from
SQL = """
    with DATA as (
        select 1 as A, '2' as B, 3 as C
        union all
        select null as A, '' as B, 13 as C
        union all
        select 21 as A, '22' as B, 23 as C
        union all
        select 31 as A, null as B, 33 as C
    )
    select B, coalesce(cast(B as long), -1) from DATA
"""
result_df = spark.sql(SQL)
result_df.show()
print(type(result_df))

# use a CTE to create a dataframe, to write to a file, to use later
SQL = """
    with DATA as (
        select 1 as A, '2' as B, 3 as C
        union all
        select null as A, '' as B, 13 as C
        union all
        select 21 as A, '22' as B, 23 as C
        union all
        select 31 as A, null as B, 33 as C
    )
    select * from DATA
"""
result_df = spark.sql(SQL)
result_df.write.format("parquet").mode("overwrite").\
    option("compression", "snappy").save("./test_data.parquet")

# use SQL to query that parquet file
# (page 98, 99 of Learning Spark)
SQL = """
    CREATE OR REPLACE  TEMPORARY VIEW  my_test_data
    USING parquet
    OPTIONS ( path "./test_data.parquet")
"""
result_df = spark.sql(SQL)
result_df.show()


SQL = """
    SELECT * from my_test_data
"""
result_df = spark.sql(SQL)
result_df.show()
