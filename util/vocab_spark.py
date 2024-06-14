
'''
    This is a class representing the OMOP CONCEPT table.
'''

from pyspark.sql import SparkSession
import os
from util.vocab_map_file import oid_map
from util.vocab_map_file import complex_mappings


# FIX TODO, this is gross:
def map_hl7_to_omop(code_system, code):
    spark = SparkSession.builder \
            .appName('CCDA_OMOP_ETL') \
            .master("local") \
            .getOrCreate()  # .config("spark.sql.warehouse.dir", self.DW_PATH) \

    return VocabSpark.map_hl7_to_omop(spark, code_system, code)


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
        self.VOCAB_FILE = './CONCEPT.csv'

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
        print(f"INFO VocabSpark.__init__() path: {dw_path}")

    def load_from_existing(self):
        # https://www.programmerall.com/article/3196638561/
        print(f"INFO VocabSpark.load_from_existing() path: {self.dw_path} cwd: {os.getcwd()}")
        sql = f"CREATE TABLE concept ({self.concept_schema}) " +\
            "USING PARQUET " +\
            "LOCATION 'concept'"
        #    "LOCATION '" + self.dw_path + "/concept'"
        #   "LOCATION '" + self.dw_path + "/ccda_omop_spark_db.db/concept'"

        df = self.spark.sql(sql)
        if (df.count() < 1):
            print(f"ERROR vocab did not load from existing files {self.dw_path}")
            print(f"INFO load sql is {sql}")
        else:
            print(f"INFO vocab seems to have loaded from  existing files {self.dw_path}")

    def load_from_csv(self):
        print(f"INFO VocabSpark.load_from_csv() path: {self.dw_path} cwd: {os.getcwd()}")
        vocab_df = self.spark.read.option('delimiter', '\t').\
            csv(self.VOCAB_FILE, schema=self.concept_schema)
        if (vocab_df.count() < 1):
            print("ERROR vocab did not load from CSV")
        else:
            print(f"INFO vocab seems to have loaded from  CSV  {self.dw_path}")
            vocab_df.write \
                .mode("overwrite") \
                .saveAsTable("concept")   # .option("path", self.DW_PATH) \

    @staticmethod
    def lookup_omop(spark, vocabulary_id, concept_code):
        """ returns an omop concpet_id from OMOP vocabulary and code values """
        sql = (f"SELECT concept_id "
               f"FROM concept "
               f"WHERE vocabulary_id = '{vocabulary_id}' "
               f"AND concept_code = '{concept_code}'")
        df = spark.sql(sql)
        # print((f"INFO: looking up {vocabulary_id}:{concept_code}"
        #        f" df is {df.count()} x {len(df.columns)}"))
        try:
            # print(f"INFO: looking up {vocabulary_id}:{concept_code} and returning {df.head()[0]}")
            return df.head()[0]
        except Exception:
            print("ERROR couldn't print df.head()[0], vocabulary likley not loaded")
            return None

    @staticmethod
    def lookup_omop_details(spark, vocabulary_id, concept_code):
        """ returns omop info from OMOP vocabulary and code values """
        sql = (f"SELECT vocabulary_id, concept_id, concept_name, domain_id, concept_class_id "
               f"FROM concept "
               f"WHERE vocabulary_id = '{vocabulary_id}'"
               f"AND concept_code = '{concept_code}'")
        df = spark.sql(sql)
        # print((f"INFO: looking up {vocabulary_id}:{concept_code} "
        #        f"df is {df.count()} x {len(df.columns)}"))
        try:
            # print(f"INFO: looking up {vocabulary_id}:{concept_code} and returning {df.head()[0]}")
            return df.head()
        except Exception:
            print("ERROR couldn't print df.head()[0], vocabulary likley not loaded")
            return None

    @staticmethod
    def map_hl7_to_omop(spark, code_system, code):
        """ returns OMOP concept_id from HL7 codeSystem and code """
        if code_system in oid_map:
            vocabulary_id = oid_map[code_system][0]
            concept_id = VocabSpark.lookup_omop(spark, vocabulary_id, code)
            print(f"INFO: omop  mapping {code_system}:{code} and returning {concept_id}")
            return concept_id

        concept_id = complex_mappings[(code_system, code)][3]
        print(f"INFO: complex mapping {code_system}:{code} and returning {concept_id}")
        return complex_mappings[(code_system, code)][3]
