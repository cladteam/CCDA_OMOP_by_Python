#!/usr/bin/env python3

# person_omop_spark.py : OmopPerson
#
# represents the OMOP v5.3 person table in Spark

"""  the database layer for  the omop person table end person entities """

import time


class PersonOmopSpark:
    """  the database layer for  the omop person table end person entities """

    BLANK_PERSON_FILE = 'resources/person_blank.csv'

    def __init__(self, spark, dw_path):
        """ sets members up, but doesn't fully initialize the table.
            That's in separate functions because it depends on the context
            set by starting Spark. That will tell us if we need to load or
            reload.
        """
        self.spark = spark
        self.dw_path = dw_path

        self.person_id = None
        self.gender_concept_id = None
        self.year_of_birth = None
        self.month_of_birth = None
        self.day_of_birth = None
        self.birth_date_time = None
        self.race_concept_id = None
        self.ethnicity_concept_id = None
        self.location_id = None
        self.provider_id = None
        self.care_site_id = None
        self.person_source_value = None
        self.gender_source_value = None
        self.gender_source_concept_id = None
        self.race_source_value = None
        self.race_source_concept_id = None
        self.ethnicity_source_value = None
        self.ethnicity_source_concept_id = None

    def _check_nulls(self):
        return self.person_id is not None and \
            self.gender_concept_id is not None and \
            self.birth_date_time is not None and \
            self.race_concept_id is not None and \
            self.ethnicity_concept_id is not None

    def populate(self, person_id, gender_concept_id, birth_date_time,
                 race_concept_id, ethnicity_concept_id, location_id):
        """ accept attribute values and populate this instance """
        assert isinstance(person_id, int)
        assert isinstance(gender_concept_id, int)
        assert isinstance(birth_date_time, str)
        assert isinstance(race_concept_id, int)
        assert isinstance(ethnicity_concept_id, int)
        assert isinstance(location_id, int)

        # TODO, (conditionally) check these against the vocabulary and appropriate PK-FK

        (year, month, day) = self.parse_time(birth_date_time)

        self.person_id = person_id                   # integer not null
        self.gender_concept_id = gender_concept_id   # integer not null
        self.year_of_birth = year                    # integer NOT NULL
        self.month_of_birth = month                  # integer NULL
        self.day_of_birth = day                      # integer NULL
        self.birth_date_time = birth_date_time       # integer Null
        self.race_concept_id = race_concept_id       # integer  not null
        self.ethnicity_concept_id = ethnicity_concept_id  # integer  not null
        self.location_id = location_id  # integer     # integer NULL
        self.provider_id = None   # integer NULL,
        self.care_site_id = None  # integer NULL,
        self.person_source_value = None  # varchar(50) NULL,
        self.gender_source_value = None   # varchar(50) NULL,
        self.gender_source_concept_id = None  # integer NULL,
        self.race_source_value = None  # varchar(50) NULL,
        self.race_source_concept_id = None  # integer NULL,
        self.ethnicity_source_value = None  # varchar(50) NULL,
        self.ethnicity_source_concept_id = None  # integer NULL

    def insert(self):
        """ insert attriute values to database  """

        if self._check_nulls():
            sql = f""" INSERT into person VALUES (
                    {self.person_id}, {self.gender_concept_id}, {self.year_of_birth},
                    {self.month_of_birth}, {self.day_of_birth}, DATE('{self.birth_date_time}'),
                    {self.race_concept_id}, {self.ethnicity_concept_id}, {self.location_id},
                    null, null, null,
                    null, null,   null, null,   null, null )
                """

        print(f"DEBUG person insert:  {sql}")
        self.spark.sql(sql)

    person_schema = """
        person_id INT,
        gender_concept_id INT,
        year_of_birth INT,
        month_of_birth INT,
        day_of_birth INT,
        birth_datetime DATE,
        race_concept_id INT,
        ethnicity_concept_id INT,
        location_id INT,
        provider_id INT,
        care_site_id INT,
        person_source_value STRING,
        gender_source_value STRING,
        gender_source_concept_id INT,
        race_source_value STRING,
        race_source_concept_id INT,
        ethnicity_source_value STRING,
        ethnicity_source_concept_id INT
    """

    # SCHEMA-level stuff
    @staticmethod
    def parse_time(datetime):
        """ parses a postgres formatted datetime into the separate
            integer fields year, month day """

        time_struct = time.strptime(datetime, '%Y-%m-%d')
        year = time.strftime('%Y', time_struct)
        month = time.strftime('%m', time_struct)
        day = time.strftime('%d', time_struct)
        return (year, month, day)

    def load_from_existing(self):
        """ loads an existing table from Spark """
        # https://www.programmerall.com/article/3196638561/
        print(">PERSON LOAD")
        sql = f"CREATE TABLE person ({PersonOmopSpark.person_schema}) " +\
            "USING PARQUET " +\
            "LOCATION '" + self.dw_path + "/ccda_omop_spark_db.db/person'"
        person_df = self.spark.sql(sql)
        print("PERSON LOAD> ", person_df.columns, person_df.count())
        # TODO some better way of checking success here?

    def load_from_csv(self):
        """ loads a csv file as a way of creating the initial Spark table """
        df = self.spark.read.option('delimiter', ',').csv(PersonOmopSpark.BLANK_PERSON_FILE,
                                                          schema=self.person_schema)
        df.write \
            .mode("overwrite") \
            .saveAsTable("person")   # .option("path", self.DW_PATH) \

    def create(self):
        """ an alternate way of creating the initial table, that doesn't work TODO """
        # https://stackoverflow.com/questions/50914102/why-do-i-get-a-hive-support-is-required-to-create-hive-table-as-select-error
        # you need either set spark.hadoop.hive.metastore.warehouse.dir
        #   if you are using embdedded metastore,
        # ...deprecated since 2.0.0, instead use spark.sql.warehouse.dir
        # spark.sql.legacy.createHiveTableByDefault to false
        print(">PERSON CREATE ")
        # ##### sql = f"CREATE TABLE person ({PersonOmopSpark.person_schema}) "
        # #####  person_df = self.spark.sql(sql)
        person_df = self.spark.createDataFrame([], schema=PersonOmopSpark.person_schema)
        person_df.write \
            .mode("overwrite") \
            .parquet(self.dw_path + "/ccda_omop_spark_db.db/person")
        #    .saveAsTable("person")
        print("PERSON CREATE> ", person_df.columns, person_df.count())

    # DICTIONARY to CSV and header (old-school?)
    def create_dictionary(self):
        """ vestigial, creates a dictionary with the attribuets
            from person, str() form used in tests """
        dest = {}

        dest['person_id'] = self.person_id
        dest['race_concept_id'] = self.race_concept_id
        dest['ethnicity_concept_id'] = self.ethnicity_concept_id
        dest['gender_concept_id'] = self.gender_concept_id
        dest['birth_datetime'] = self.birth_date_time
        dest['location_id'] = self.location_id

        return dest

    @staticmethod
    def create_header():
        """ creates a csv header (used?) """
        print("person_id, gender_concept_id, " +
              "year_of_birth, month_of_birth, day_of_birth, birth_datetime, " +
              "race_concept_id, ethnicity_concept_id, location_id, provider_id, " +
              "care_site_id, person_source_value, " +
              "gender_source_value, gender_source_concept_id,  race_source_value, " +
              "race_source_concept_id, " +
              "ethnicity_source_value, ethnicity_source_concept_id")

    def create_csv_line(self):
        """ creates a csv line (used?) """

        print(f"{self.person_id}, {self.gender_concept_id}, " +
              "{self.year_of_birth}, {self.month_of_birth}, {self.day_of_birth}, " +
              "{self.birth_date_time}, " +
              "{self.race_concept_id}, {self.ethnicity_concept_id}, {self.location_id}, "
              "{self.provider_id}, {self.care_site_id}, {self.person_source_value}, " +
              "{self.gender_source_value}, {self.gender_source_concept_id}, " +
              "{self.race_source_value}, {self.race_source_concept_id}, " +
              "{self.ethnicity_source_value}, {self.ethnicity_source_concept_id}")
