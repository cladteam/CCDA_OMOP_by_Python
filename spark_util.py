
# ~/work/data/omop_vocabulary
# /Users/roederc/work/git_learn/learn_spark

"""
    manages and initializes the spark connection and schema
"""

from pyspark.sql import SparkSession
import vocab_spark
from table_objects import person_omop_spark


class SparkUtil():
    """
    A place to keep a SparkSession and from which to get them...
    There are two issues going on here. Is Spark running? and is there an existing DW?
    - No running SparkSession and when starting it, there is no data to reload.
    - No running SparkSession, but there are tables in the DW to relaod.
    - There is a running seesion, just get a new object to forward.
    """

    SCHEMA = 'ccda_omop_spark_db'
    DW_PATH = "/Users/roederc/work/data"

    def __init__(self):
        """ sets up constants and starts Spark if necessary """

        self.spark = SparkSession.builder \
            .appName('CCDA_OMOP_ETL') \
            .config("spark.hadoop.hive.metastore.warehouse.dir", SparkUtil.DW_PATH) \
            .config("spark.sql.warehouse.dir", SparkUtil.DW_PATH) \
            .config("spark.sql.legacy.createHiveTableByDefault", False) \
            .master("local") \
            .getOrCreate()

        # Odd that you try to start and if that fails you restart. Backwards? TODO
        self.start()

    def get_spark(self):  # should this be some kind of getterless attribute TODO
        """ returns a spark session """
        # futhermore Spark has session management built in, so you can just ask Spark for a spark.
        # This class should only have to deal with the re-load,
        # and not try to re-write session pooling, poorly.
        return self.spark

    # https://stackoverflow.com/questions/48416385/how-to-read-spark-table-back-again-in-a-new-spark-session
    # https://stackoverflow.com/questions/70700195/load-spark-bucketed-table-from-disk-previously-written-via-saveastable?rq=3

    def start(self):
        """ sets up the tables used (deploy  might be a better name? TODa """
        self.spark.sql("CREATE DATABASE ccda_omop_spark_db")
        self.spark.sql("USE ccda_omop_spark_db")

        # once for each table
        print("CONCEPT")
        vocab_obj = vocab_spark.VocabSpark(self.spark, SparkUtil.DW_PATH)
        try:
            vocab_obj.load_from_csv()
        except Exception:
            vocab_obj.load_from_existing()

        print("PERSON")
        person_obj = person_omop_spark.PersonOmopSpark(self.spark, SparkUtil.DW_PATH)
        try:
            person_obj.load_from_csv()  # person_obj.create()
        except Exception as e:
            print("WARNING: creating person failed, loading existing instead of creating new", e)
            try:
                person_obj.load_from_existing()
                print("INFO: loading person seems to have workd", e)
            except Exception as e2:
                print("ERROR: loading person failed", e2)

        print("DONE")

    def _prep_person(self):
        """ junk for later """
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
        """ junk for later """
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
        """ junk for later """
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
        """ junk for later """
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
