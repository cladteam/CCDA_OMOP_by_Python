#!/usr/bin/env python3

import argparse
import os
import pandas as PD
import logging
import prototype_2.data_driven_parse as DDP
from prototype_2.metadata import get_meta_dict


logger = logging.getLogger(__name__)


def create_omop_domain_dataframes(omop_data, filepath):
    """ transposes the rows into columns,
        creates a Pandas dataframe
    """
    df_dict = {}
    for domain_name, domain_list in omop_data.items():
        # Transpose to a dictoinary of named columns.

        # Initialize a dictionary of columns from the first row
        column_dict = {}
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
        domain_df = PD.DataFrame(column_dict)
        df_dict[domain_name] = domain_df

    return df_dict


def write_csvs_from_dataframe_dict(df_dict, file_name, folder):
    """ writes a CSV file for each dataframe
        uses the key of the dict as filename
    """
    for domain_name, domain_dataframe in df_dict.items():
        domain_dataframe.to_csv(f"{folder}/{file_name}__{domain_name}.csv",
                                sep=",", header=True, index=False)


def process_file(filepath):
    print(f"PROCESSING {filepath} ")
    logger.info(f"PROCESSING {filepath} ")
    base_name = os.path.basename(filepath)

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"logs/log_layer_ds_{base_name}.log",
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


def main():

    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
    args = parser.parse_args()

    if args.filename is not None:
        process_file(args.filename)
    elif args.directory is not None:
        only_files = [f for f in os.listdir(args.directory) if os.path.isfile(os.path.join(args.directory, f))]
        for file in (only_files):
            if file.endswith(".xml"):
                process_file(os.path.join(args.directory, file))
    else:
        logger.error("Did args parse let us  down? Have neither a file, nor a directory.")

    if False:  # for getting them on the Foundry
        from foundry.transforms import Dataset
        ccd_ambulatory = Dataset.get("ccda_ccd_b1_ambulatory_v2")
        ccd_ambulatory_files = ccd_ambulatory.files().download()
        ccd_ambulatory_path = ccd_ambulatory_files['CCDA_CCD_b1_Ambulatory_v2.xml']



if __name__ == '__main__':
    main()
