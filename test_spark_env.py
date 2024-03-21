#!/usr/bin/env python3

import spark_env

my_spark_env = spark_env.SparkEnv()
my_spark_env.restart()


sql = "SELECT * from concept where lower(name) like '%systolic%'"

sparkenv = my_spark_env.get_spark()
result_df = sparkenv.sql(sql)
print(result_df.count())
result_df.show()
