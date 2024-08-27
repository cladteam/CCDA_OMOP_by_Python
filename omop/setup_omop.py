""" setup_omop
    Initiates an in-memory instance of DuckDB, reads in the OMOP DDL,
    and reads in any data provided.

    For now, it's useful to see issues regarding  PK presence and uniqueness, datatypes..

    TODO: This includes abuse of the OMOP DDL.  Better solutions  include
    - better metadata so the resulting dataset and CSV look like OMOP
    - a second stage here that modifies the resulting datasets to look more
      like OMOP
    - some compromise means getting a handle on how narrow the CSV can be
      compared to OMOP. Can you leave out unused nullable fields?
"""

OMOP_CDM_DIR =  "../CommonDataModel/inst/ddl/5.3/duckdb/"
OMOP_CSV_DATA_DIR = "output/"

import io
import os
import re
import duckdb

conn = duckdb.connect()

sql_import_dict = {
    'Person': {
        'sql': """
                INSERT INTO TABLENAME SELECT date_of_birth, gender_concept_id, race_concept_id,
                        ethnicity_concept_id, person_id
                FROM  read_csv('FILENAME', delim=',', header=True)
               """,
        'table_name': "person",
        'pk_query': """
                SELECT count(*) as row_ct, count(person_id) as p_id,
                            count(distinct person_id) as d_p_id
                FROM person
                """
    },
    'Visit': {
        'sql': """
                INSERT INTO TABLENAME SELECT person_id, visit_occurrence_id, care_site_id,
                    provider_id, start_time, end_time, visit_concept_id
                FROM  read_csv('FILENAME', delim=',', header=True)
               """,
        'table_name': "visit_occurrence",
        'pk_query': """
                SELECT count(*) as row_ct, count(visit_occurrence_id) as p_id, count(distinct visit_occurrence_id) as d_p_id
                FROM visit_occurrence
                """
    },
    'Measurement': {
        'sql': """
                INSERT INTO TABLENAME SELECT person_id, visit_occurrence_id, measurement_time, value_as_string, value_type,
                    value_unit, measurement_concept_id, value_as_number, value_as_concept_id, measurement_id
                FROM  read_csv('FILENAME', delim=',', header=True)
               """,
        'table_name': "measurement",
        'pk_query': """
                SELECT count(*) as row_ct, count(measurement_id) as p_id, count(distinct measurement_id) as d_p_id
                FROM measurement
                """
    }
}

# date_of_birth,gender_concept_id,race_concept_id,ethnicity_concept_id,person_id
person_ddl= """
CREATE OR REPLACE TABLE person (
            birth_datetime DATE NULL,
            gender_concept_id integer NULL, --  this is NOT NULL officially TODO
            race_concept_id integer NULL, -- NOT NULL officially,
            ethnicity_concept_id integer NULL, --  NOT NULL officially
            person_id varchar(100) NOT NULL -- integer  officially TODO
            --year_of_birth integer NOT NULL,
            -- month_of_birth integer NULL,
            --day_of_birth integer NULL,
            --location_id integer NULL,
            --provider_id integer NULL,
            --care_site_id integer NULL,
            --person_source_value varchar(50) NULL,
            --gender_source_value varchar(50) NULL,
            --gender_source_concept_id integer NULL,
            --race_source_value varchar(50) NULL,
            --race_source_concept_id integer NULL,
            --ethnicity_source_value varchar(50) NULL,
            --ethnicity_source_concept_id integer NULL 
);
"""


# person_id,visit_occurrence_id,care_site_id,provider_id,start_time,end_time,visit_concept_id
visit_ddl = """
CREATE TABLE visit_occurrence (
            person_id varchar(100) NOT NULL, --  person_id integer NOT NULL,
            visit_occurrence_id integer NULL, -- NOT NULL,
            care_site_id varchar(30) NULL, -- care_site_id integer NULL,
            provider_id varchar(30) NULL, -- provider_id integer NULL,
            visit_start_date date NOT NULL,
            visit_end_date date NOT NULL,
            visit_concept_id integer NULL  -- NOT NULL
            -- visit_start_date date NOT NULL,
            -- visit_start_datetime TIMESTAMP NULL,
            -- visit_end_date date NOT NULL,
            -- visit_end_datetime TIMESTAMP NULL,
            -- visit_type_concept_id Integer NOT NULL,
            -- visit_source_value varchar(50) NULL,
            -- visit_source_concept_id integer NULL,
            -- admitting_source_concept_id integer NULL,
            -- admitting_source_value varchar(50) NULL,
            -- discharge_to_concept_id integer NULL,
            -- discharge_to_source_value varchar(50) NULL,
            -- preceding_visit_occurrence_id integer NULL 
);
"""

# person_id,visit_occurrence_id,measurement_time,value_as_string,value_type,value_unit,measurement_concept_id,value_as_number,value_as_concept_id,measurement_id
measurement_ddl = """
CREATE TABLE measurement (
            person_id varchar(100) NOT NULL, -- person_id integer NOT NULL,
            visit_occurrence_id integer NULL,
            measurement_time varchar(10) NULL,
            value_as_string varchar(100), -- bogus
            value_type varchar(100), -- bogus
            value_unit varchar(100), -- unit_concept_id integer NULL,
            measurement_concept_id integer NULL, -- NOT NULL,
            value_as_number NUMERIC NULL,
            value_as_concept_id integer NULL,
            measurement_id varchar(50) NOT NULL  -- measurement_id integer NOT NULL 
            -- measurement_date date NOT NULL,
            -- measurement_datetime TIMESTAMP NULL,
            -- measurement_type_concept_id integer NOT NULL,
            -- operator_concept_id integer NULL,
            -- range_low NUMERIC NULL,
            -- range_high NUMERIC NULL,
            -- provider_id integer NULL,
            -- visit_detail_id integer NULL,
            -- measurement_source_value varchar(50) NULL,
            -- measurement_source_concept_id integer NULL,
            -- unit_source_value varchar(50) NULL, 
            -- value_source_value varchar(50) NULL 
);
"""

def _apply_local_ddl():
    x=conn.execute(person_ddl)
    x=conn.execute(visit_ddl)
    x=conn.execute(measurement_ddl)
    df = conn.sql("SHOW ALL TABLES;").df()
    print(df[['database', 'schema', 'name']])


def _apply_ddl():
    with io.open(OMOP_CDM_DIR +  "OMOPCDM_duckdb_5.3_ddl.sql", "r") as ddl_file:
        ddl_statements = ddl_file.read()
        i=0
        for statement in ddl_statements.split(";"):
            statement = statement.replace("@cdmDatabaseSchema.", "") + ";"
            print(f"DDL:{statement}")
            x=conn.execute(statement)
            print(x.df())

            #df = conn.sql("SHOW ALL TABLES;").df()
            #print(df[['database', 'schema', 'name']])
            print("-----------------------------------")
            i= i+1
            if i>0:
                break

        print("i======================================")
        df = conn.sql("SHOW ALL TABLES;").df()
        print(df[['database', 'schema', 'name']])

    print("o======================================")
    df = conn.sql("SHOW ALL TABLES;").df()
    print(df[['database', 'schema', 'name']])


def _import_CSVs(domain):
    files = [f for f in os.listdir(OMOP_CSV_DATA_DIR) if os.path.isfile(os.path.join(OMOP_CSV_DATA_DIR, f)) ]
    files = [f for f in files if  re.match('.*' + f"{domain}" + '.csv',f) ]
    for csv_filename in files:
        try:
            sql_string = sql_import_dict[domain]['sql']
            table_name = sql_import_dict[domain]['table_name']
            sql_string = sql_string.replace('FILENAME', OMOP_CSV_DATA_DIR + csv_filename)
            sql_string = sql_string.replace('TABLENAME', table_name)
            try:
                x=conn.execute(sql_string)
                print(f"Loaded {domain} from {csv_filename}")
            except Exception as e:
                print(f"Failed to load {domain} from {csv_filename}")
            #print(x.df())
        except duckdb.BinderException as e:
            print(f"Failed to read {csv_filename} {type(e)} {e}")


def check_PK(domain):
    table_name = sql_import_dict[domain]['table_name']
    pk_query = sql_import_dict[domain]['pk_query']
    table_name = sql_import_dict[domain]['table_name']
    df=conn.sql(f"SELECT * from  {table_name}").df()
    df=conn.sql(pk_query).df()
    if df['row_ct'][0] != df['p_id'][0]:
        print("ERROR row count not the same as id count, null IDs?")
    if df['p_id'][0] != df['d_p_id'][0]:
        print("ERROR id count not the same as distinct ID count, non-unique IDs?")




if __name__ == '__main__':
    #_apply_ddl()
    _apply_local_ddl()

    _import_CSVs('Person')
    check_PK('Person')

    _import_CSVs('Visit')
    check_PK('Visit')

    _import_CSVs('Measurement')
    check_PK('Measurement')

    if False:
        df = conn.sql("SHOW ALL TABLES;").df()
        print(df[['database', 'schema', 'name']])
        print(list(df))

        df = conn.sql("SHOW TABLES;").df()
        print('"' + df['name'] + '"')

    exit()


