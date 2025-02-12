#!/usr/bin/env python3

import argparse
import os
import pandas as pd
import logging
from typeguard import typechecked
try:
    from foundry.transforms import Dataset
except Exception:
    print("no foundry transforms imported")
from collections import defaultdict
import lxml
from prototype_2.ddl import sql_import_dict
from prototype_2.ddl import config_to_domain_name_dict

import prototype_2.data_driven_parse as DDP
from prototype_2.metadata import get_meta_dict


""" layer_datasets.py
    This is a layer over data_driven_parse.py that takes the 
    dictionary of lists of dictionaries, a dictionary of rows
    where the keys are dataset_names. It converts these structures
    to pandas dataframes and then merges dataframes destined for /
    the same domain. Reason being that multiple places in CCDA
    generate data for the same OMOP domain. It then publishes
    the dataframes as datasets into the Spark world in Foundry.
"""

logger = logging.getLogger(__name__)


@typechecked
def show_column_dict(config_name, column_dict):
    for key,val in column_dict.items():
        print(f"   config: {config_name}  key:{key} length(val):{len(val)}")


def find_max_columns(config_name :str, domain_list: list[ dict[str, tuple[ None | str | float | int, str]] | None  ]) -> dict[str, any]:
    """  Give a list of dictionaries, find the maximal set of columns that has the basic OMOP columns. 

         Trying to deal with a list that may have dictionaries that lack certain fields.
         An option is to go with a completely canonical list, like from the DDL, but we want
         to remain flexible and be able to easily add columns that are not part of the DDL for 
         use later in Spark. It is also true that we do load into an RDB here, DuckDB, to 
         check PKs and FK constraints, but only on the OMOP columns. The load scripts there
         use the DDL and ignore columns to the right we want to allow here.
    """
    domain = None
    try:
        domain = config_to_domain_name_dict[config_name]
    except Exception as e:
        print(f"ERROR no domain for {config_name} in {config_to_domain_name_dict.keys()}")
        print("The config_to_domain_name_dict in ddl.py probably needs this to be added to it.")
        raise e

    chosen_row =-1
    num_columns = 0
    row_num=-1
    for col_dict in domain_list:
        # Q1 does  this dict at least have what the ddl expects?
        good_row = True
        for key in sql_import_dict[domain]['column_list']:
            if key not in col_dict:
                    good_row = False
        # Q2: does it have the most extra
        if good_row and len(col_dict.keys()) > num_columns:
            chosen_row = row_num
        row_num += 1
    return domain_list[row_num]


@typechecked
def create_omop_domain_dataframes(omop_data: dict[str, list[ dict[str,  None | str | float | int] | None  ] | None],
                                  filepath) ->  dict[str, pd.DataFrame]:
    """ transposes the rows into columns,
        creates a Pandas dataframe
    """
    df_dict = {}
    for config_name, domain_list in omop_data.items():
        # Transpose to a dictionary of named columns.

        # Initialize a dictionary of columns from schema
        if domain_list is None or len(domain_list) < 1:
            logger.error(f"No data to create dataframe for {config_name} from {filepath}")
            print(f"ERROR No data to create dataframe for {config_name} from {filepath}")
        else:
            column_list = find_max_columns(config_name, domain_list)
            column_dict =  dict((k, []) for k in column_list) #dict.fromkeys(column_list)

            # Add the data from all the rows
            if domain_list is None or len(domain_list) < 1:
                logger.error(f"No data when creating datafame for {config_name} from {filepath}")
                print(f"No data when creating datafame for {config_name} from {filepath}")
            else:
                for domain_data_dict in domain_list:
                    for field in column_dict.keys():
                        if field in domain_data_dict:
                            column_dict[field].append(domain_data_dict[field])
                        else:
                            column_dict[field].append(None)
    
            # create a Pandas dataframe from the data_dict
            try:
                ##show_column_dict(config_name, column_dict)
                domain_df = pd.DataFrame(column_dict)
                df_dict[config_name] = domain_df
            except ValueError as ve:
                logger.error(f"ERROR when creating dataframe for {config_name} \"{ve}\"")
                print(f"ERROR when creating dataframe for {config_name} \"{ve}\"")
                show_column_dict(config_name, column_dict)
                df_dict[config_name] = None
    

    return df_dict


@typechecked
def write_csvs_from_dataframe_dict(df_dict :dict[str, pd.DataFrame], file_name, folder):
    """ writes a CSV file for each dataframe
        uses the key of the dict as filename
    """
    for config_name, domain_dataframe in df_dict.items():
        filepath = f"{folder}/{file_name}__{config_name}.csv"
        if domain_dataframe is not None:
            domain_dataframe.to_csv(filepath, sep=",", header=True, index=False)
        else:
            print(f"ERROR: NOT WRITING domain {config_name} to file {filepath}, no dataframe")


@typechecked
def process_file(filepath) -> dict[str, pd.DataFrame]:
    """ processes file, creates dataset and writes csv
        returns dataset
    """
    base_name = os.path.basename(filepath)

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"logs/log_file_{base_name}.log",
        force=True,
        # level=logging.ERROR
        level=logging.WARNING
        # level=logging.INFO
        # level=logging.DEBUG
    )

    omop_data = DDP.parse_doc(filepath, get_meta_dict())
    DDP.reconcile_visit_foreign_keys(omop_data)
    # DDP.print_omop_structure(omop_data)
    if omop_data is not None or len(omop_data) < 1:
        dataframe_dict = create_omop_domain_dataframes(omop_data, filepath)
    else:
        logger.error(f"no data from {filepath}")
    write_csvs_from_dataframe_dict(dataframe_dict, base_name, "output")

    return dataframe_dict


@typechecked
def dict_summary(my_dict):
    for key in my_dict:
        logger.info(f"Summary {key} {len(mh_dict[key])}")


@typechecked
def build_file_to_domain_dict(meta_config_dict :dict[str, dict[str, dict[str, str]]]) -> dict[str, str]:
    """ The meta_config_dict is a dictionary keyed by domain filenames that
        has the data that drives the conversion. Included is a 'root' element
        that has an attribute 'expected_domain_id' that we're after to identify
        the OMOP domain that a file's data is destined for.
        This is where multiple files for the same domain get combined.     
        
        For example, the Measurement domain, rows for the measurement table can 
        come from at least two kinds of files:
         <file>__Measurement_results.csv
         <file>__Measurement_vital_signs.csv
         
       This map maps from filenames to domains
    """
    file_domain_map = {} 
    for file_domain in meta_config_dict:
        file_domain_map[file_domain] = meta_config_dict[file_domain]['root']['expected_domain_id']
    return file_domain_map

@typechecked
def export_to_foundry(dataset_name, df, max_retries=5):
    try:
        for attempt in range(1, max_retries + 1):  # Allow up to 5 attempts
            try:
                export_dataset = Dataset.get(dataset_name)
                export_dataset.write_table(df)
                print(f"Successfully exported dataset '{dataset_name}'")
                return
            except Exception as e:
                print(f"    ERROR: {e}")
                error_message = str(e)
                # Detect column name dynamically
                if "Conversion failed for column" in error_message or "RECONCILE FK" in error_message:
                    col_name = error_message.split("column ")[1].split(" with type")[0].strip("'")
                    print(f"    Converting '{dataset_name}' '{col_name}' to string...")
                    # Convert the affected column to string
                    df[col_name] = df[col_name].astype(str)
                    print(f"    RETRY '{attempt}':' Exporting {dataset_name}")
    except Exception as e:
        print(f"    FAILED: to export dataset '{dataset_name}' after {max_retries} attempts.")
        print(f"    ERROR: {e}")
        print("")

def main():
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP parser with datasets layer layer_datasets.py',
        description="reads CCDA XML, translate to and writes OMOP CSV files",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
    parser.add_argument('-x', '--export', action=argparse.BooleanOptionalAction, help="export to foundry")
    args = parser.parse_args()

    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    
    # Single File, put the datasets into the omop_dataset_dict
    if args.filename is not None:
        process_file(args.filename)
        
    # Do a Directory, process a file and concat it with previous dataset into the omop_dataset_dict
    elif args.directory is not None:
        only_files = [f for f in os.listdir(args.directory) if os.path.isfile(os.path.join(args.directory, f))]
        for file in (only_files):
            if file.endswith(".xml"):
                new_data_dict = process_file(os.path.join(args.directory, file))
                for key in new_data_dict:
                    if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                        if new_data_dict[key] is  not None:
                            omop_dataset_dict[key] = pd.concat([ omop_dataset_dict[key], new_data_dict[key] ])
                    else:
                        omop_dataset_dict[key]= new_data_dict[key]
                    if new_data_dict[key] is not None:
                        logger.info(f"{file} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
                    else:
                        logger.info(f"{file} {key} {len(omop_dataset_dict)} None / no data")
    else:
        logger.error("Did args parse let us  down? Have neither a file, nor a directory.")

    # COMBINE like datasets
    # We need to collect all files/datasets that have the same expected_domain_id.
    # For example, the Measurement domain, rows for the measurement table can 
    # come from at least two kinds of files:
    #     <file>__Measurement_results.csv
    #     <file>__Measurement_vital_signs.csv
    # Two dictionaries at play here:
    # 1 is the omop_dataset_dict which is a dictionary of datasets keyed by their domain_keys or config filenames
    # 2 is the config data that comes from get_meta_dict
    file_to_domain_dict = build_file_to_domain_dict(get_meta_dict())
    domain_dataset_dict = {}
    for filename in omop_dataset_dict:
        print(f"key:{filename} {omop_dataset_dict[filename].shape} ")
        domain_id = file_to_domain_dict[filename]
        if domain_id in domain_dataset_dict:
            domain_dataset_dict[domain_id] = pd.concat([ domain_dataset_dict[domain_id], omop_dataset_dict[filename] ])
        else:
            domain_dataset_dict[domain_id] = omop_dataset_dict[filename]
            
    # write the combined CSV files
    for domain_id in domain_dataset_dict:
        print(f" domain:{domain_id} dim:{domain_dataset_dict[domain_id].shape}")
        domain_dataset_dict[domain_id].to_csv(f"output/domain_{domain_id}.csv")

    if args.export:
        # export the datasets to Spark/Foundry
        print("EXPORT enabled")
        for domain_id in domain_dataset_dict:
            dataset_name = domain_id.lower()
            print(f"EXPORTING: {dataset_name}")
            export_to_foundry(dataset_name,domain_dataset_dict[domain_id])
            
if __name__ == '__main__':
    main()
