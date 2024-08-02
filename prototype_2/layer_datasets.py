#!/usr/bin/env python3

import os.path
import pandas as PD
import logging
import data_driven_parse as DDP
from metadata import get_meta_dict


logger = logging.getLogger('basic logging')

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

def write_csvs_from_dataframe_dict(df_dict, file_name):
    """ writes a CSV file for each dataframe 
        uses the key of the dict as filename
    """
    for domain_name, domain_dataframe in df_dict.items():
        domain_dataframe.to_csv(file_name + "__" + domain_name + ".csv", sep=",", header=True, index=False)

if __name__ == '__main__':
  # GET FILE
    file_paths = [
#        '../resources/CCDA_CCD_b1_Ambulatory_v2.xml',
#        '../resources/CCDA_CCD_b1_InPatient_v2.xml',
        '../resources/170.314b2_AmbulatoryToC.xml',
##       '../resources/ToC_CCDA_CCD_CompGuideSample_FullXML.xml',
##       '../resources/Manifest_Medex/bennis_shauna_ccda.xml', # missing : in XML from ElementTree.parse()
#        '../resources/Manifest_Medex/eHX_Terry.xml',
#        '../resources/CRISP Content Testing Samples/CRISP Main Node/anna_flux.xml',
#        '../resources/CRISP Content Testing Samples/HealtheConnect Alaska/healtheconnectak-ccd-20210226.2.xml'
    ]

    if False: # for getting them on the Foundry
        from foundry.transforms import Dataset
        ccd_ambulatory = Dataset.get("ccda_ccd_b1_ambulatory_v2")
        ccd_ambulatory_files = ccd_ambulatory.files().download()
        ccd_ambulatory_path = ccd_ambulatory_files['CCDA_CCD_b1_Ambulatory_v2.xml']

    for filepath in file_paths:
        print(f"PROCESSING {filepath} ")
        omop_data = DDP.parse_doc(filepath, get_meta_dict()) 
        #DDP.print_omop_structure(omop_data) 
        if omop_data is not None or len(omop_data) < 1:
            dataframe_dict = create_omop_domain_dataframes(omop_data, filepath)
        else:
            print(f"ERROR sorry pal, no data from {filepath}")
        file_name = os.path.basename(filepath)
#        write_csvs_from_dataframe_dict(dataframe_dict, file_name)


