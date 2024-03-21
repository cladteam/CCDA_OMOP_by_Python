
# ~/work/data/omop_vocabulary
# /Users/roederc/work/git_learn/learn_spark

from pyspark.sql import SparkSession

class SparkEnv(object):
   

    def __init__(self):
       self.spark = SparkSession.builder \
           .master("local") \
           .appName('CCDA-OMOP-ETL') \
           .getOrCreate()
          #.config("spark.some.config.option", "some-value") \
       self.VOCAB_FILE = '/Users/roederc/work/data/omop_vocabulary'

    def __del__(self):  
        self.spark.stop()

    def _create_new_spark_env(self):
        self.spark.sql("CREATE DATABASE CCDA_OMOP_spark_db")
        self.spark.sql("USE CCDA_OMOP_spark_db")

        #spark.sql("DROP TABLE if exists learn_spark_db.people")


    def _load_vocab(self):
        vocab_schema = """
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
        vocab_df = self.spark.read.csv(self.VOCAB_FILE, vocab_schema)
        vocab_df.write.mode("overwrite").saveAsTable("concept")

    def _prep_person(self):
        schema = """
            person_id integer NOT NULL, 
            gender_concept_id integer NOT NULL,
            year_of_birth integer NOT NULL,
            month_of_birth integer NULL,
            day_of_birth integer NULL, 
            birth_datetime TIMESTAMP NULL,
            race_concept_id integer NOT NULL,
            ethnicity_concept_id integer NOT NULL,
            location_id integer NULL,
            provider_id integer NULL,
            care_site_id integer NULL,
            person_source_value varchar(50) NULL,
            gender_source_value varchar(50) NULL,
            gender_source_concept_id integer NULL,
            race_source_value varchar(50) NULL,
            race_source_concept_id integer NULL,
            ethnicity_source_value varchar(50) NULL,
            ethnicity_source_concept_id integer NULL );
        """


    def _prep_observation(self):
        schema = """
            observation_id integer NOT NULL,
            person_id integer NOT NULL,
            observation_concept_id integer NOT NULL,
            observation_date date NOT NULL,
            observation_datetime TIMESTAMP NULL,
            observation_type_concept_id integer NOT NULL,
            value_as_number NUMERIC NULL,
            value_as_string varchar(60) NULL,
            value_as_concept_id Integer NULL,
            qualifier_concept_id integer NULL,
            unit_concept_id integer NULL,
            provider_id integer NULL,
            visit_occurrence_id integer NULL,
            visit_detail_id integer NULL,
            observation_source_value varchar(50) NULL,
            observation_source_concept_id integer NULL,
            unit_source_value varchar(50) NULL, 
            qualifier_source_value varchar(50) NULL );
        """

    def _prep_location(self):
        schema = """
            location_id integer NOT NULL,
            address_1 varchar(50) NULL,
            address_2 varchar(50) NULL,
            city varchar(50) NULL,
            state varchar(2) NULL,
            zip varchar(9) NULL,
            county varchar(20) NULL,
            location_source_value varchar(50) NULL );
        """


    def _prep_visit_occurrence(self):
        schema = """
            visit_occurrence_id integer NOT NULL,
            person_id integer NOT NULL, 
            visit_concept_id integer NOT NULL,
            visit_start_date date NOT NULL,
            visit_start_datetime TIMESTAMP NULL,
            visit_end_date date NOT NULL,
            visit_end_datetime TIMESTAMP NULL,
            visit_type_concept_id Integer NOT NULL,
            provider_id integer NULL,
            care_site_id integer NULL,
            visit_source_value varchar(50) NULL,
            visit_source_concept_id integer NULL,
            admitting_source_concept_id integer NULL,
            admitting_source_value varchar(50) NULL,
            discharge_to_concept_id integer NULL,
            discharge_to_source_value varchar(50) NULL,
            preceding_visit_occurrence_id integer NULL );
        """


    def restart(self):
        self._create_new_spark_env()
        self._load_vocab()

    def get_spark(self): # should this be some kind of getterless attribute TODO
        return self.spark    

