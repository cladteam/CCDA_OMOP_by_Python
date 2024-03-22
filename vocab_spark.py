
# ~/work/data/omop_vocabulary
# /Users/roederc/work/git_learn/learn_spark

from pyspark.sql import SparkSession
from os.path import abspath

class VocabSpark(object):
    """ 
    A place to keep the concept table initialization and re-load.
    maybe better as just a package. It feels a little Java-y
    (note the static methods maybe?) TODO
    """   
    def __init__(self, spark, dw_path):
        """ sets members up, but doesn't fully initialize the table.
            That's in separate functions because it depends on the context
            set by starting Spark. That will tell us if we need to load or
            reload.
        """
        self.spark = spark
        self.dw_path = dw_path
        self.VOCAB_FILE = '/Users/roederc/work/data/omop_vocabulary/CONCEPT.csv'

        self.concept_schema = """
            concept_id INT, 
            concept_name STRING, 
            domain_id STRING, 
            vocabulary_id STRING, 
            concept_class_id STRING,
            standard_concept STRING,
            concept_code STRING, 
            valid_start_date DATE, 
            valid_end_date DATE, 
            invalid_reason STRING
        """

    def load(self):
        # https://www.programmerall.com/article/3196638561/
        sql =  f"CREATE TABLE concept ({self.concept_schema}) " +\
            "USING PARQUET " +\
            "LOCATION '" + self.dw_path + "/ccda_omop_spark_db.db/concept'"
        
        result_thing = self.spark.sql(sql)
        print(result_thing)


    def reload(self):
        vocab_df = self.spark.read.option('delimiter', '\t').csv(self.VOCAB_FILE, schema=self.concept_schema)
        vocab_df.write \
            .mode("overwrite") \
            .saveAsTable("concept")
            # .option("path", self.DW_PATH) \


