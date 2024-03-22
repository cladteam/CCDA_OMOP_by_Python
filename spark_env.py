
# ~/work/data/omop_vocabulary
# /Users/roederc/work/git_learn/learn_spark

from pyspark.sql import SparkSession
from os.path import abspath

class SparkEnv(object):
   

    def __init__(self):
        self.VOCAB_FILE = '/Users/roederc/work/data/omop_vocabulary/CONCEPT.csv'
        self.SCHEMA = 'ccda_omop_spark_db'
        self.DW_PATH = "/Users/roederc/work/data"

        self.spark = SparkSession.builder \
            .appName('CCDA_OMOP_ETL') \
            .config("spark.sql.warehouse.dir", self.DW_PATH) \
            .master("local") \
            .getOrCreate()
            #.enableHiveSupport() \

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
    

    def __del__(self):  
        self.spark.stop()

    def _create_new_spark_env(self):
        self.spark.sql("CREATE DATABASE ccda_omop_spark_db")
        self.spark.sql("USE ccda_omop_spark_db")

        #spark.sql("DROP TABLE if exists learn_spark_db.people")

    def start(self):
        self._create_new_spark_env()
        self._load_concept()
        #try:
        #    self._create_new_spark_env()
        #    self._load_concept()
        #except:
        #    print("""INFO: Spark failed to start. Try either just starting it, or 
        #            deleting the spark-warehouse/ccda_omop_spark_db.db directory""")
        #    raise

    def restart(self):  # TODO
        # https://stackoverflow.com/questions/48416385/how-to-read-spark-table-back-again-in-a-new-spark-session
        # https://stackoverflow.com/questions/70700195/load-spark-bucketed-table-from-disk-previously-written-via-saveastable?rq=3
        print("RE-STARTING")
        self._re_load_concept()
        print("RE-START-ED")

    def get_spark(self): # should this be some kind of getterless attribute TODO
        return self.spark    




    def _re_load_concept(self):
        # https://www.programmerall.com/article/3196638561/
        sql =  f"CREATE TABLE concept ({self.concept_schema}) " +\
            "USING PARQUET " +\
            "LOCATION '" + self.DW_PATH + "/ccda_omop_spark_db.db/concept'"
        
        result_thing = self.spark.sql(sql)
        print(result_thing)

    def _load_concept(self):
        vocab_df = self.spark.read.option('delimiter', '\t').csv(self.VOCAB_FILE, schema=self.concept_schema)
        vocab_df.write \
            .mode("overwrite") \
            .saveAsTable("concept")
            # .option("path", self.DW_PATH) \

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


