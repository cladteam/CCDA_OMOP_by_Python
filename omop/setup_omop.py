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

OMOP_CDM_DIR = "resources/" #  "../CommonDataModel/inst/ddl/5.3/duckdb/"
OMOP_CSV_DATA_DIR = "output/"

import io
import os
import re
import logging
import duckdb

conn = duckdb.connect()
logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(levelname)s: %(message)s',
    filename=f"logs/load_omop.log",
    force=True,
    level=logging.INFO
    # level=logging.WARNING level=logging.ERROR # level=logging.INFO # level=logging.DEBUG
)

sql_import_dict = {
    'Person': {
        'sql': """
                INSERT INTO TABLENAME SELECT
            person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
            birth_datetime, race_concept_id, ethnicity_concept_id,
            location_id, provider_id, care_site_id, person_source_value,
            gender_source_value, gender_source_concept_id, race_source_value,
            race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id
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
                INSERT INTO TABLENAME SELECT
            person_id, visit_occurrence_id, care_site_id, provider_id,
            visit_start_date, visit_end_date, visit_concept_id,
            visit_start_date, visit_start_datetime, visit_end_date,
            visit_end_datetime, visit_type_concept_id, visit_source_value,
            visit_source_concept_id, admitting_source_concept_id,
            admitting_source_value, discharge_to_concept_id,
            discharge_to_source_value, preceding_visit_occurrence_id
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
                INSERT INTO TABLENAME SELECT 
            person_id, visit_occurrence_id, measurement_time, value_as_string,
            value_type, value_unit, measurement_concept_id, value_as_number,
            value_as_concept_id, measurement_id, measurement_date, measurement_datetime,
            measurement_type_concept_id, operator_concept_id, range_low, range_high,
            provider_id, visit_detail_id, measurement_source_value,
            measurement_source_concept_id, unit_source_value, value_source_value
                FROM  read_csv('FILENAME', delim=',', header=True)
               """,
        'table_name': "measurement",
        'pk_query': """
                SELECT count(*) as row_ct, count(measurement_id) as p_id, count(distinct measurement_id) as d_p_id
                FROM measurement
                """
    }
}


def _apply_local_ddl():
    x=conn.execute(person_ddl)
    x=conn.execute(visit_ddl)
    x=conn.execute(measurement_ddl)
    df = conn.sql("SHOW ALL TABLES;").df()
    print(df[['database', 'schema', 'name']])


def _apply_ddl(ddl_file):
    print(f"\nApplying DDL file {ddl_file}")
    with io.open(OMOP_CDM_DIR +  ddl_file, "r") as ddl_file:
        ddl_statements = ddl_file.read()
        for statement in ddl_statements.split(";"):
            statement = statement.replace("@cdmDatabaseSchema.", "") + ";"
            x=conn.execute(statement)


    print("o======================================")
    df = conn.sql("SHOW ALL TABLES;").df()
    print(df[['database', 'schema', 'name']])


def _import_CSVs(domain):
    print(f"\nImporting domain {domain} data")
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
                logger.info(f"Loaded {domain} from {csv_filename}")
            except Exception as e:
                logger.error(f"Failed to load {domain} from {csv_filename}")
                logger.error(e)
            #print(x.df())
        except duckdb.BinderException as e:
            logger.error(f"Failed to read {csv_filename} {type(e)} {e}")


def check_PK(domain):
    print(f"\nChecking PK on domain {domain} ")
    table_name = sql_import_dict[domain]['table_name']
    pk_query = sql_import_dict[domain]['pk_query']
    table_name = sql_import_dict[domain]['table_name']
    df=conn.sql(f"SELECT * from  {table_name}").df()
    df=conn.sql(pk_query).df()
    if df['row_ct'][0] != df['p_id'][0]:
        logger.error("row count not the same as id count, null IDs?")
    if df['p_id'][0] != df['d_p_id'][0]:
        logger.error("id count not the same as distinct ID count, non-unique IDs?")



def main():
    #_apply_ddl("OMOPCDM_duckdb_5.3_ddl.sql")
    _apply_ddl("OMOPCDM_duckdb_5.3_ddl_with_constraints.sql")
    
    _import_CSVs('Person')
    check_PK('Person')
    
    _import_CSVs('Visit')
    check_PK('Visit')

    _import_CSVs('Measurement')
    check_PK('Measurement')
    
    #_import_CSVs('Observation')
    #check_PK('Observation')
    
    # not implemented in ALTER TABLE yet in v1.0
    # https://github.com/OHDSI/CommonDataModel/issues/713 
##    _apply_ddl("OMOPCDM_duckdb_5.3_primary_keys.sql")
##    _apply_ddl("OMOPCDM_duckdb_5.3_constraints.sql")
    _apply_ddl("OMOPCDM_duckdb_5.3_indices.sql")
    
    check_PK('Person')

    if False:
        df = conn.sql("SHOW ALL TABLES;").df()
        print(df[['database', 'schema', 'name']])
        print(list(df))

        df = conn.sql("SHOW TABLES;").df()
        print('"' + df['name'] + '"')

    exit()

if __name__ == '__main__':
    main()


