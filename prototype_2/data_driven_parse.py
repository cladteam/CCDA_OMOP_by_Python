#!/usr/bin/env python3

""" Table-Driven ElementTree parsing in Python

 This version puts the paths into a data structure and explores using
 one function driven by the data. 
 - The mapping_dict is hard-coded here. An next step would be to read that in from a file.
 - Value transformation is stubbed out waiting for vocabularies to be loaded, and to 
   figure out how to use them once there.
 
 Chris Roeder
 2024-07-25: needs access to vocabulary, needs to do multiple obsrvations, needs dervied values from code on foundry
 2024-07-26: needs test driver, the main function needs broken out into file, field and attribute
- change tags to be values of a 'tag' key so it looks more like a column in a csv
"""

import pandas as pd
# mamba install -y -q lxml
import lxml
from lxml import etree as ET
import logging
from metadata import get_meta_dict

logger = logging.getLogger('basic logging')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.ERROR)
console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.DEBUG)
console_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(levelname)s %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


ns = {
   '': 'urn:hl7-org:v3',  # default namespace
   'hl7': 'urn:hl7-org:v3',
   'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
   'sdtc': 'urn:hl7-org:sdtc'
}


PK_dict = {}



def parse_field_from_dict(field_details_dict, domain_root_element, domain, field_tag):
    if 'element' not in field_details_dict:
        logger.error(f"could find key 'element' in the field_details_dict: {field_details_dict}")
    else:
        logger.info(f"    FIELD {field_details_dict['element']} for {domain}/{field_tag}")
        field_element = domain_root_element.find(field_details_dict['element'], ns)
        if field_element is None:
            logger.error(f"could find field element {field_details_dict['element']} for {domain}/{field_tag}")
            return None
    
        if 'attribute' not in field_details_dict:
            logger.error(f"could not find key 'attribute' in the field_details_dict: {field_details_dict}")
        else: 
            logger.info(f"       ATTRIBUTE   {field_details_dict['attribute']} for {domain}/{field_tag} {field_details_dict['element']} ")
            attribute_value = field_element.get(field_details_dict['attribute'])
            #if attribute_value is None and field_details_dict['attribute'] == "#text":
            if field_details_dict['attribute'] == "#text":
                attribute_value = field_element.text
            if attribute_value is None:
                logger.warning(f"no value for field element {field_details_dict['element']} for {domain}/{field_tag}")
            return(attribute_value)
    

def parse_domain_from_dict(tree, domain, domain_meta_dict):
    """ The main logic is here. 
        Given a tree from ElementTree representing a CCDA document (ClinicalDocument, not just file),
        parse the different domains out of it, linking PK and FKs between them.
        Return a dictionary, keyed by domain name, of dictionaries of rows, each a dictionary
        of fields.
    """
    # Find root
    if 'root' not in domain_meta_dict:
        logger.error(f"DOMAIN {domain} lacks a root element.")
        return None

    if 'element' not in domain_meta_dict['root']:
        logger.error(f"DOMAIN {domain} root lacks an 'element' key.")
        return None

    logger.info(f"DOMAIN domain:{domain} root:{domain_meta_dict['root']['element']}")
    root_element_list = tree.findall(domain_meta_dict['root']['element'], ns)
    if root_element_list is None or len(root_element_list) == 0: 
        logger.error(f"DOMAIN couldn't find root element for {domain} with {domain_meta_dict['root']['element']}")
        return None

    
    # Do others. 
    # Watch for two kinds of errors: 
    #  1) the metadata has mis-spelled keys (Python domain)
    #  2) the paths that appear as values to 'element' keys don't work in ElementTree (XPath domain)
    output_list = []
    print(f"NUM ROOTS {len(root_element_list)}")
    for root_element in root_element_list:
        output_dict = {}
        logger.info(f"  ROOT for domain:{domain}, we have tag:{root_element.tag} attributes:{root_element.attrib}")

        for (field_tag, field_details_dict) in domain_meta_dict.items():
            logger.info(f"     FIELD domain:'{domain}' field_tag:'{field_tag}' {field_details_dict}")
            type_tag = field_details_dict['type']
            if type_tag == 'FIELD':
                logger.info(f"     FIELD for {domain}/{field_tag}")
                attribute_value = parse_field_from_dict(field_details_dict, root_element, domain, field_tag)
                output_dict[field_tag] = attribute_value
            elif type_tag == 'PK':
                logger.info(f"     PK for {domain}/{field_tag}")
                attribute_value = parse_field_from_dict(field_details_dict, root_element, domain, field_tag)
                output_dict[field_tag] = attribute_value
                PK_dict[field_tag] = attribute_value
            elif type_tag == 'FK':
                logger.info(f"     FK for {domain}/{field_tag}")
                if field_tag in PK_dict:
                    output_dict[field_tag] = PK_dict[field_tag] 
                else:
                    logger.error(f"could not find {field_tag}  in PK_dict for {domain}/{field_tag}")
                    output_dict[field_tag] = None

        # Do derived values now that their inputs should be available in the output_dict                           
        for (field_tag, field_details_dict) in domain_meta_dict.items():
            if field_details_dict['type'] == 'DERIVED':
                logger.info(f"     DERIVING {field_tag}, {field_details_dict}")
                # NB Using an explicit dict here instead of kwargs because this code here
                # doesn't know what the keywords are at 'compile' time.
                args_dict = {}
                for arg_name, field_name in field_details_dict['argument_names'].items():
                    logger.info(f"     -- {field_tag}, arg_name:{arg_name} field_name:{field_name}")
                    args_dict[arg_name] = output_dict[field_name]
                output_dict[field_tag] = field_details_dict['FUNCTION'](args_dict)
                logger.info(f"     DERIV-ed {field_tag}, {field_details_dict} {output_dict[field_tag]}")

        output_list.append(output_dict)
        
    return output_list
  
 
def parse_doc(file_path, metadata):
    """ Parses many domains from a single file, collects them
        into a dict to return.
    """
    omop_dict = {}
    tree = ET.parse(file_path)
    for domain, domain_meta_dict in metadata.items():
        domain_data_dict =  parse_domain_from_dict(tree, domain, domain_meta_dict)
        omop_dict[domain] = domain_data_dict
    return omop_dict


def print_omop_structure(omop):
    """ prints a dict of parsed domains as returned from parse_doc()
        or parse_domain_from_dict()
    """
    print(f"PK_dict: {PK_dict}")
    for domain, domain_list in omop.items():
        for domain_data_dict in domain_list:
            print(f"\n\nDOMAIN: {domain}")
            for field, parts in domain_data_dict.items():
                print(f"    FIELD:{field} VALUE:{parts}")

if __name__ == '__main__':  
    
    # GET FILE
    ccd_ambulatory_path = '../resources/CCDA_CCD_b1_Ambulatory_v2.xml'
    if False:
        from foundry.transforms import Dataset
        ccd_ambulatory = Dataset.get("ccda_ccd_b1_ambulatory_v2")
        ccd_ambulatory_files = ccd_ambulatory.files().download()
        ccd_ambulatory_path = ccd_ambulatory_files['CCDA_CCD_b1_Ambulatory_v2.xml']
 
    data = parse_doc(ccd_ambulatory_path, get_meta_dict()) 
    print_omop_structure(data) 
