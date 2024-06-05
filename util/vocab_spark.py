
# ~/work/data/omop_vocabulary
# /Users/roederc/work/git_learn/learn_spark

from pyspark.sql import SparkSession


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
        #self.VOCAB_FILE = '/Users/roederc/work/data/omop_vocabulary/CONCEPT.csv'
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

    # HL7: codeSyste, code --> OMOP: vocabulary_id, concept_code, name, concept_id
    # HL7: codeSyste, code --> OMOP: vocabulary_id, concept_code, name, concept_id
    complex_mappings = {
        ('2.16.840.1.113883.5.1', 'F'): ("Gender", 'F', "FEMALE", 8532),
        ('2.16.840.1.113883.5.1', 'M'): ("Gender", 'M', "MALE", 8532),

        ("2.16.840.1.113883.6.238", "2106-3"): ("Race", "5", "White", 8527),

        ("2.16.840.1.113883.6.238", "2186-5"):
            ("Ethnicity", "Not Hispanic", "Not Hispanic or Latino", 38003564),
        ("2.16.840.1.113883.6.238", None):
            ("Ethnicity", "Hispanic", "Hispanic or Latino", 9998, 38003563)
    }

    equivalent_vocab_map = {
        '2.16.840.1.113883.6.1': "LOINC",
        "http://snomed.info/sct": "SNOMED"
    }

    def load_from_existing(self):
        # https://www.programmerall.com/article/3196638561/
        print(f"ORIG {self.dw_path}")
        sql = f"CREATE TABLE concept ({self.concept_schema}) " +\
            "USING PARQUET " +\
            "LOCATION '" + self.dw_path + "/concept'"

        #   "LOCATION '" + self.dw_path + "/ccda_omop_spark_db.db/concept'"

        result_thing = self.spark.sql(sql)
        print(result_thing)  # TODO some better way of checking success here?

    def load_from_csv(self):
        vocab_df = self.spark.read.option('delimiter', '\t').csv(self.VOCAB_FILE, schema=self.concept_schema)
        vocab_df.write \
            .mode("overwrite") \
            .saveAsTable("concept")   # .option("path", self.DW_PATH) \

    @staticmethod
    def lookup_omop(spark, vocabulary_id, concept_code):
        """ returns an omop concpet_id from OMOP vocabulary and code values """
        sql = f"SELECT concept_id from concept where vocabulary_id = '{vocabulary_id}' and concept_code = '{concept_code}'"
        df = spark.sql(sql)
        print(f"INFO: looking up {vocabulary_id}:{concept_code}")
        try:
            print(f"INFO: looking up {vocabulary_id}:{concept_code} and returning {df.head()[0]}")
        except:
            print("WARN couldn't print df.head()[0]")
        return df.head()[0]

    # def map_hl7_to_omop(self, code_system, code):
    #     return map_hl7_to_omop(self.spark, code_system, code)

    @staticmethod
    def map_hl7_to_omop(spark, code_system, code):
        """ returns OMOP concept_id from HL7 codeSystem and code """
        if code_system in VocabSpark.equivalent_vocab_map:
            vocabulary_id = VocabSpark.equivalent_vocab_map[code_system]
            # concept_id = omop_concept_ids[(vocabulary_id, code)][1]
            concept_id = VocabSpark.lookup_omop(spark, vocabulary_id, code)
            print(f"INFO: omop  mapping {code_system}:{code} and returning {concept_id}")
            return concept_id

        concept_id = VocabSpark.complex_mappings[(code_system, code)][3]
        print(f"INFO: complex mapping {code_system}:{code} and returning {concept_id}")
        return VocabSpark.complex_mappings[(code_system, code)][3]

