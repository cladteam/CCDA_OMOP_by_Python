""" setup_omop
    Initiates an in-memory instance of DuckDB, reads in the OMOP DDL,
    and reads in any data provided.
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
                INSERT INTO TABLENAME SELECT date_of_birth, gender_concept_id, race_concept_id, ethnicity_concept_id, person_id
                FROM  read_csv('FILENAME', delim=',', header=True)
               """,
        'sql_orig': """INSERT INTO TABLENAME 
                 SELECT person_id, gender_concept_id, -999, -12, -9, date_of_birth,  race_concept_id, ethnicity_concept_id,
                        0, 1, 2, 'person_source_value', 'gender_source_value', -88, 'race_source_value', -77, 
                        'ethnicity_source_value', -66
                 FROM  read_csv('FILENAME', delim=',', header=True)
               """,
        'table_name': "person",
        'pk_query': "SELECT count(*) as row_ct, count(person_id) as p_id, count(distinct person_id) as d_p_id"
    }
}

# date_of_birth,gender_concept_id,race_concept_id,ethnicity_concept_id,person_id
person_ddl= """
CREATE OR REPLACE TABLE person (
            birth_datetime DATE NULL,
            gender_concept_id integer NULL, --  this is NOT NULL officially TODO
            race_concept_id integer NULL, -- NOT NULL officially,
            ethnicity_concept_id integer NULL, --  NOT NULL officially
            person_id varchar(100) NOT NULL); -- integer  officially TODO
"""

""",
            year_of_birth integer NOT NULL,
            month_of_birth integer NULL,
            day_of_birth integer NULL,
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


def _create_just_person():
    x=conn.execute(person_ddl)
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
            print(csv_filename)
            sql_string = sql_import_dict[domain]['sql']
            table_name = sql_import_dict[domain]['table_name']
            sql_string = sql_string.replace('FILENAME', OMOP_CSV_DATA_DIR + csv_filename)
            sql_string = sql_string.replace('TABLENAME', table_name)
            #print(sql_string)
            x=conn.execute(sql_string)
            #print(x.df())
        except duckdb.BinderException as e:
            print(f"Failed to read {csv_filename} {type(e)} {e}")


def check_PK(domain):
    table_name = sql_import_dict[domain]['table_name']
    pk_query = sql_import_dict[domain]['pk_query']
    df=conn.sql(f"SELECT * from  {table_name}").df()
    print(df)
    df=conn.sql(pk_query).df()
    print(df)


if __name__ == '__main__':
    #_apply_ddl()
    _create_just_person()
    _import_CSVs('Person')

    if False:
        df = conn.sql("SHOW ALL TABLES;").df()
        print(df[['database', 'schema', 'name']])
        print(list(df))

        df = conn.sql("SHOW TABLES;").df()
        print('"' + df['name'] + '"')

    check_PK('Person')
    exit()


#OMOPCDM_duckdb_5.3_constraints.sql
#OMOPCDM_duckdb_5.3_indices.sql
#OMOPCDM_duckdb_5.3_primary_keys.sql
