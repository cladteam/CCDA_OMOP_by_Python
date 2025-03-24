#!/usr/bin/env python3

import argparse
import logging
import os
import pandas as pd
import re
from typeguard import typechecked
try:
    from foundry.transforms import Dataset
except Exception:
    print("no foundry transforms imported")
from collections import defaultdict
import lxml
import tempfile
from numpy import int32
from numpy import float32
from numpy import datetime64
import numpy as np
import warnings




from prototype_2.ddl import sql_import_dict
from prototype_2.ddl import config_to_domain_name_dict
from prototype_2.ddl import domain_name_to_table_name
import prototype_2.data_driven_parse as DDP
from prototype_2.metadata import get_meta_dict
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types 


""" layer_datasets.py
    This is a layer over data_driven_parse.py that takes the 
    dictionary of lists of dictionaries, a dictionary of rows
    where the keys are dataset_names. It converts these structures
    to pandas dataframes and then merges dataframes destined for /
    the same domain. Reason being that multiple places in CCDA
    generate data for the same OMOP domain. It then publishes
    the dataframes as datasets into the Spark world in Foundry.
    
    Run 
        - from dataset named "ccda_documents" with export:
            bash> python3 -m prototype_2.layer_datasets -ds ccda_documents -x
        - from directory named "resources" without export:
            bash> python3 -m prototype_2.layer_datasets -d resources
"""


#****************************************************************
#*                                                              *
warnings.simplefilter(action='ignore', category=FutureWarning) #*
#*                                                              * 
#****************************************************************
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
            ###print(f"ERROR No data to create dataframe for {config_name} from {filepath}")
        else:
            column_list = find_max_columns(config_name, domain_list)
            column_dict =  dict((k, []) for k in column_list) #dict.fromkeys(column_list)

            # Add the data from all the rows
            for domain_data_dict in domain_list:
                for field in column_dict.keys():
                    if field in domain_data_dict:
                        if domain_data_dict[field] == 'RECONCILE FK':
                            logger.error(f"RECONCILE FK for {field} in {config_name}")
                            column_dict[field].append(None)
                        elif field == 'visit_concept_id' and type(domain_data_dict[field]) == str:
                            # hack when visit_type_xwalk returns a string
                            column_dict[field].append(int32(domain_data_dict[field]))
                        elif field[-8:] == "datetime" and domain_data_dict[field] is not None:
                            try:
                                column_dict[field].append(domain_data_dict[field].replace(tzinfo=None))
                            except Exception as e:
                                print(f"ERROR  TZ {type(domain_data_dict[field])} {domain_data_dict[field]} {field} {e}")
                        else:
                            column_dict[field].append(domain_data_dict[field])
                    else:
                        column_dict[field].append(None)
                        
    
            # use domain_dataframe_colunn_types to cast as directed
            # Q: this just adds 0 and casts those specifically?
            # Create a Pandas dataframe from the data_dict
            try:
                ##show_column_dict(config_name, column_dict)
                domain_df = pd.DataFrame(column_dict)
                domain_name = config_to_domain_name_dict[config_name]
                table_name = domain_name_to_table_name[domain_name]
                if table_name in domain_dataframe_column_types.keys():
                    for column_name, column_type in domain_dataframe_column_types[table_name].items():
                        
                        
                        if column_type in ['date', 'datetime', datetime64]:
                            domain_df[column_name] = pd.to_datetime(domain_df[column_name])
  
                            if column_type == 'date':
                                try:
                                    domain_df[column_name] = domain_df[column_name].dt.date # datetime still
                                    #domain_df[column_name] = domain_df[column_name].dt.date.astype('object') -- leaves as integer!
                                except Exception as e:
                                    print(f"CAST ERROR (to date)  in layer_datasets.py table:{table_name} column:{column_name} type:{column_type}  ")
                                    print(f"  exception  {domain_df[column_name]}   {type(domain_df[column_name])}")
                        else:
                            try:
                                domain_df[column_name] = domain_df[column_name].fillna(0).astype(column_type)  # generates downcasting wwarnings and doesn't throw, 
                                # domain_df[column_name] = domain_df[column_name].fillna(cast(column_type, 0)).astype(column_type)  # throwss
                                #domain_df[column_name] = domain_df[column_name].astype(column_type).fillna(0) # cast errors on the None
                            except Exception as e:
                                print(f"CAST ERROR in layer_datasets.py table:{table_name} column:{column_name} type:{column_type}  ")
                                print(f"  exception  {domain_df[column_name]}   {type(domain_df[column_name])}")
                    
                                                              
                            
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
def process_file(filepath, write_csv_flag) -> dict[str, pd.DataFrame]:
    """ processes file, creates dataset and writes csv
        returns dataset
    """
    base_name = os.path.basename(filepath)

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"logs/log_file_{base_name}.log",
        force=True,
         level=logging.ERROR
        #level=logging.WARNING
        # level=logging.INFO
        # level=logging.DEBUG
    )

#    print(f"   parsing {filepath}")
    omop_data = DDP.parse_doc(filepath, get_meta_dict())
#    print("   reconciling")
    DDP.reconcile_visit_foreign_keys(omop_data)
    # DDP.print_omop_structure(omop_data)
    if omop_data is not None or len(omop_data) < 1:
#        print("    creating dataframes")
        dataframe_dict = create_omop_domain_dataframes(omop_data, filepath)
        
    else:
        logger.error(f"no data from {filepath}")
        
    if write_csv_flag:
 #       print("   writing CSVs")
        write_csvs_from_dataframe_dict(dataframe_dict, base_name, "output")
#    print("   done")
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
def export_to_foundry(domain_name, df):
    """
    exports domains to datasets in Foundry.
    """
    
    if domain_name not in domain_name_to_table_name:
        printf("ERROR: not able to map domain:{domain_name} to dataset/table name")

    dataset_name = domain_name_to_table_name[domain_name]
    print(f"EXPORTING: {dataset_name}")
    try:
        export_dataset = Dataset.get(dataset_name)
        export_dataset.write_table(df)
        print(f"Successfully exported dataset '{dataset_name}'")
    except Exception as e:
        print(f"    ERROR: {e}")
        error_message = str(e)

        
def combine_datasets(omop_dataset_dict):    
    
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
        ###print(f"key:{filename} {omop_dataset_dict[filename].shape} ")
        domain_id = file_to_domain_dict[filename]
        if domain_id in domain_dataset_dict:
            domain_dataset_dict[domain_id] = pd.concat([ domain_dataset_dict[domain_id], omop_dataset_dict[filename] ])
        else:
            domain_dataset_dict[domain_id] = omop_dataset_dict[filename]      
            
    return domain_dataset_dict


def do_export_datasets(domain_dataset_dict):
    # export the datasets to Spark/Foundry
    for domain_id in domain_dataset_dict:
        print(f"Exporting dataset for domain:{domain_id} dim:{domain_dataset_dict[domain_id].shape}")
        export_to_foundry(domain_id, domain_dataset_dict[domain_id])      

        
def do_write_csv_files(domain_dataset_dict):
    for domain_id in domain_dataset_dict:
        print(f"Writing CSV for domain:{domain_id} dim:{domain_dataset_dict[domain_id].shape}")
        domain_dataset_dict[domain_id].to_csv(f"output/domain_{domain_id}.csv")
 

        
# ENTRY POINT for dataset of files
def process_dataset_of_files(dataset_name, export_datasets, write_csv_flag, limit, skip):
    print("starting dataset:{dataset_name} export:{export_datasets} csv:{write_csv_flag} limit:{limit}")
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    
    ccda_documents = Dataset.get(dataset_name)
    print(ccda_documents.files())
    ccda_documents_generator = ccda_documents.files()
    skip_count=0
    file_count=0
    for filegen in ccda_documents_generator:
        if skip>0 and skip_count < skip:
            skip_count+=1
            #print(f"skipping {os.path.basename(filegen)} {type(filegen)}")
            print(f"skipping  {skip_count} {type(filegen)}")
        else:
            if limit == 0 or file_count < limit:
                filepath = filegen.download()
                
                print(f"PROCESSING {file_count} {os.path.basename(filepath)}  {file_count}  export:{export_datasets} csv:{write_csv_flag} limit:{limit}")
                new_data_dict = process_file(filepath, write_csv_flag)
                
                for key in new_data_dict:
                    if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                        if new_data_dict[key] is  not None:
                            omop_dataset_dict[key] = pd.concat([ omop_dataset_dict[key], new_data_dict[key] ])
                    else:
                        omop_dataset_dict[key]= new_data_dict[key]
                    if new_data_dict[key] is not None:
                        logger.info(f"{filepath} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
                    else:
                        logger.info(f"{filepath} {key} {len(omop_dataset_dict)} None / no data")
                file_count += 1
            else:
                break
            
    domain_dataset_dict = combine_datasets(omop_dataset_dict)
#    print(f"\n\nEXPORTING?  export:{export_datasets} csv:{write_csv_flag} \n")
    if write_csv_flag:
        print(f"Writing CSV for input dataset: :q{dataset_name}")
        do_write_csv_files(domain_dataset_dict)

    if export_datasets:
        print(f"Exporting dataset for {dataset_name}") 
        do_export_datasets(domain_dataset_dict)
    
    
# ENTRY POINT for dataset of strings
def process_dataset_of_strings(dataset_name, export_datasets, write_csv_flag):
    print(f"DATA SET NAME: {dataset_name}")
    
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    ccda_ds = Dataset.get(dataset_name)
    ccda_df = ccda_ds.read_table(format='pandas')
    # columns: 'timestamp', 'mspi', 'site', 'status_code', 'response_text',
    # FOR EACH ROW
    if True:
        text=ccda_df.iloc[0,4]
        print("====")
        doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
        # (don't close the opening tag because it has attributes)
        # works: doc_regex = re.compile(r'(<section>.*?</section>)', re.DOTALL)
        
        # FOR EACH "DOC" in this row (hopefully just 1)
        i=0
        for match in doc_regex.finditer(text):
            match_tuple = match.groups(0)
            with tempfile.NamedTemporaryFile() as temp:
                file_path = temp.name
                with open(file_path, 'w') as f:
                    f.write(match_tuple[0]) # .encode())
                    f.seek(0)
                    
                    new_data_dict = process_file(file_path, write_csv_flag)
                    
                    for key in new_data_dict:
                        if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                            if new_data_dict[key] is  not None:
                                omop_dataset_dict[key] = pd.concat([ omop_dataset_dict[key], new_data_dict[key] ])
                        else:
                            omop_dataset_dict[key]= new_data_dict[key]
                        if new_data_dict[key] is not None:
                            logger.info((f"{file_path} {key} {len(omop_dataset_dict)} "
                                          "{omop_dataset_dict[key].shape} {new_data_dict[key].shape}"))
                        else:
                            logger.info(f"{file_path} {key} {len(omop_dataset_dict)} None / no data")
            
    domain_dataset_dict = combine_datasets(omop_dataset_dict)
    if write_csv_flag:
        do_write_csv_files(domain_dataset_dict)

    if export_datasets:
        do_export_datasets(domain_dataset_dict)
    
    
# ENTRY POINT for directory of files
def process_directory(directory_path, export_datasets, write_csv_flag):
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    
    only_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    for file in (only_files):
        if file.endswith(".xml"):
            new_data_dict = process_file(os.path.join(directory_path, file), write_csv_flag)
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
                    
    domain_dataset_dict = combine_datasets(omop_dataset_dict)
    if write_csv_flag:
        do_write_csv_files(domain_dataset_dict)

    if export_datasets:
        do_export_datasets(domain_dataset_dict)
         

# ENTRY POINT
def main():
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP parser with datasets layer layer_datasets.py',
        description="reads CCDA XML, translate to and writes OMOP CSV files",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
    group.add_argument('-ds', '--dataset', help="dataset to parse")
    parser.add_argument('-x', '--export', action=argparse.BooleanOptionalAction, help="export to foundry")
    parser.add_argument('-c', '--write_csv', action=argparse.BooleanOptionalAction, help="write CSV files to local")
    #parser.add_argument('-l', '--limit', action=argparse.BooleanOptionalAction, type=int, help="max files to process")  #, default=0)
    parser.add_argument('-l', '--limit', type=int, help="max files to process", default=0)
    parser.add_argument('-s', '--skip', type=int, help="files to skip before processing to limit, -s 100 ", default=0) 
    args = parser.parse_args()
    print(f"got args:  dataset:{args.dataset} export:{args.export} csv:{args.write_csv} limit:{args.limit}")
    print(args)
    
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    
    if True:
        # Single File, put the datasets into the omop_dataset_dict
        if args.filename is not None:
            process_file(args.filename, args.write_csv)
            
        elif args.directory is not None:
            domain_dataset_dict = process_directory(args.directory, args.export, args.write_csv)
        elif args.dataset is not None:
            domain_dataset_dict = process_dataset_of_files(args.dataset, args.export, args.write_csv, args.limit, args.skip)
        else:
            logger.error("Did args parse let us  down? Have neither a file, nor a directory.")

            
if __name__ == '__main__':
    main()
