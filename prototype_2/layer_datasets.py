#!/usr/bin/env python3

import argparse
import os
import pandas as pd
import logging
from typeguard import typechecked
from foundry.transforms import Dataset
from collections import defaultdict
import lxml

import prototype_2.data_driven_parse as DDP
from prototype_2.metadata import get_meta_dict


""" layer_datasets.py
    This is a layer over data_driven_parse.py that takes the 
    dictionary of lists of dictionaries, a dictionary of rows
    where the keys are dataset_names. It converts these structures
    to pandas dataframes and then merges dataframes destined for 
    the same domain. Reason being that multiple places in CCDA
    generate data for the same OMOP domain. It then publishes
    the dataframes as datasets into the Spark world in Foundry.
"""

logger = logging.getLogger(__name__)


@typechecked
def show_column_dict(column_dict):
    for key,val in column_dict.items():
        print(f" key:{key} length(val):{len(val)}")


@typechecked
def create_omop_domain_dataframes(omop_data: dict[str, list[ dict[str, tuple[ None | str | float | int, str]] | None  ] | None],
                                  filepath) ->  dict[str, pd.DataFrame]:
    """ transposes the rows into columns,
        creates a Pandas dataframe
    """
    df_dict = {}
    for domain_name, domain_list in omop_data.items():
        # Transpose to a dictoinary of named columns.

        # Initialize a dictionary of columns from the first row
        #column_dict = {}
        column_dict = defaultdict(list)
        if domain_list is None or len(domain_list) < 1:
            logger.error(f"No data for {domain_name} from {filepath}")
        else:
            for field, parts in domain_list[0].items():
                column_dict[field] = []

        # Add the data from all the rows
        if domain_list is None or len(domain_list) < 1:
            logger.error(f"No data for {domain_name} from {filepath}")
        else:
            for domain_data_dict in domain_list:
                for field, parts in domain_data_dict.items():
                    column_dict[field].append(parts[0])

        # create a Pandas dataframe from the data_dict
        try:
            domain_df = pd.DataFrame(column_dict)
        except ValueError as ve:
            logger.error(f"ERROR {ve}")
            show_column_dict(column_dict)
        df_dict[domain_name] = domain_df

    return df_dict


@typechecked
def write_csvs_from_dataframe_dict(df_dict :dict[str, pd.DataFrame], file_name, folder):
    """ writes a CSV file for each dataframe
        uses the key of the dict as filename
    """
    for domain_name, domain_dataframe in df_dict.items():
        domain_dataframe.to_csv(f"{folder}/{file_name}__{domain_name}.csv",
                                sep=",", header=True, index=False)


@typechecked
def process_file(filepath) -> dict[str, pd.DataFrame]:
    """ processes file, creates dataset and writes csv
        returns dataset
    """
    logger.info(f"PROCESSING {filepath} ")
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


def main():

    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
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
                    logger.info(f"{file} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
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

    # export the datasets to Spark/Foundry
    for domain_id in domain_dataset_dict:
        dataset_name = domain_id.lower()
        print(f"Exporting dataframe \"{domain_id}\" to Foundry dataset \"{dataset_name}\"")
        try:
            export_dataset = Dataset.get(dataset_name)
            export_dataset.write_table(domain_dataset_dict[domain_id])
        except Exception as e:
            print(f"  ERROR {e}")
            print("")
    
    
if __name__ == '__main__':
    main()
