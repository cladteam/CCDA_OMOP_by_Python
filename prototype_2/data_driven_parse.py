#!/usr/bin/env python3

""" Table-Driven ElementTree parsing in Python

 This version puts the paths into a data structure and explores using
 one function driven by the data. 
 - The mapping_dict is hard-coded here. An next step would be to read that in from a file.
 - Value transformation is stubbed out waiting for vocabularies to be loaded, and to 
   figure out how to use them once there.
 
 Chris Roeder

2024-07-31: visit_concept_id is incorrect, It's picking up pnemonia a Condition when we want a Visit domain_id
"           need to run on multiple files, need to bring over the test script and correct_files,
"           test script needs sophistication to correlate input with expected output
"""

import pandas as pd
# mamba install -y -q lxml
import lxml
from lxml import etree as ET
import logging
from metadata import get_meta_dict

logger = logging.getLogger('basic logging')
#logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
logger.setLevel(logging.ERROR)

console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.DEBUG)
#console_handler.setLevel(logging.INFO)
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



def parse_field_from_dict(field_details_dict, domain_root_element, domain, field_tag, root_path):
    """ Retrieves a value for the field descrbied in field_details_dict that lies below
        the domain_root_element. 
        Domain and field_tag are here for error messages.
    """
    if 'element' not in field_details_dict:
        logger.error(f"could find key 'element' in the field_details_dict: {field_details_dict} root:{root_path}")
    else:
        logger.info(f"    FIELD {field_details_dict['element']} for {domain}/{field_tag}")
        field_element = domain_root_element.find(field_details_dict['element'], ns)
        if field_element is None:
            logger.error(f"could not find field element {field_details_dict['element']} for {domain}/{field_tag} root:{root_path}")
            return None
    
        if 'attribute' not in field_details_dict:
            logger.error(f"could not find key 'attribute' in the field_details_dict: {field_details_dict} root:{root_path}")
        else: 
            logger.info(f"       ATTRIBUTE   {field_details_dict['attribute']} for {domain}/{field_tag} {field_details_dict['element']} ")
            attribute_value = field_element.get(field_details_dict['attribute'])
            if field_details_dict['attribute'] == "#text":
                attribute_value = field_element.text
            if attribute_value is None:
                logger.warning(f"no value for field element {field_details_dict['element']} for {domain}/{field_tag} root:{root_path}")
            return(attribute_value)
    

def parse_domain_from_dict(tree, domain, domain_meta_dict):
    """ The main logic is here. 
        Given a tree from ElementTree representing a CCDA document (ClinicalDocument, not just file),
        parse the different domains out of it, linking PK and FKs between them.
        Returns a list, output_list, of dictionaries, output_dict, keyed by field name, containing a list of the value and the path to it:
            [ { field_1 : (value, path), field_2: (value, path)}, { field_1: (value, path)}, {field_2: (value, path)} ]
        It's a list of because you might have more than one instance of the root path, like when you
        get many observations.
    """
    # Find root
    if 'root' not in domain_meta_dict:
        logger.error(f"DOMAIN {domain} lacks a root element.")
        return None

    if 'element' not in domain_meta_dict['root']:
        logger.error(f"DOMAIN {domain} root lacks an 'element' key.")
        return None

    root_path = domain_meta_dict['root']['element']
    logger.info(f"DOMAIN >>  domain:{domain} root:{domain_meta_dict['root']['element']}   ROOT path:{root_path}")
    root_element_list = tree.findall(domain_meta_dict['root']['element'], ns)
    if root_element_list is None or len(root_element_list) == 0: 
        logger.error(f"DOMAIN couldn't find root element for {domain} with {domain_meta_dict['root']['element']}")
        return None

    
    # Do others.
    # Also look for the first DERIVED value that is tagged "MATCH_DOMAIN" and keep its domain_id.
    # Watch for two kinds of errors: 
    #  1) the metadata has mis-spelled keys (Python domain)
    #  2) the paths that appear as values to 'element' keys don't work in ElementTree (XPath domain)
    output_list = []
    domain_id = None
    print(f"NUM ROOTS {len(root_element_list)}")
    for root_element in root_element_list: 
        output_dict = {}
        logger.info(f"  ROOT for domain:{domain}, we have tag:{root_element.tag} attributes:{root_element.attrib}")

        for (field_tag, field_details_dict) in domain_meta_dict.items():
            logger.info(f"     FIELD domain:'{domain}' field_tag:'{field_tag}' {field_details_dict}")
            type_tag = field_details_dict['type']
            if type_tag == 'FIELD':
                logger.info(f"     FIELD for {domain}/{field_tag}")
                attribute_value = parse_field_from_dict(field_details_dict, root_element, domain, field_tag, root_path)
                output_dict[field_tag] = (attribute_value, root_path + "/" + field_details_dict['element'] + "/@" + field_details_dict['attribute'])
            elif type_tag == 'PK':
                logger.info(f"     PK for {domain}/{field_tag}")
                attribute_value = parse_field_from_dict(field_details_dict, root_element, domain, field_tag, root_path)
                output_dict[field_tag] = (attribute_value, root_path + "/" + field_details_dict['element'] + "/@" + field_details_dict['attribute'])
                PK_dict[field_tag] = attribute_value
            elif type_tag == 'FK':
                logger.info(f"     FK for {domain}/{field_tag}")
                if field_tag in PK_dict:
                    output_dict[field_tag] = (PK_dict[field_tag], 'FK')
                else:
                    logger.error(f"could not find {field_tag}  in PK_dict for {domain}/{field_tag}")
                    output_dict[field_tag] = (None, root_path + "/" + field_details_dict['element'] + "/@" + field_details_dict['attribute'])
                

        # Do derived values now that their inputs should be available in the output_dict                           
        for (field_tag, field_details_dict) in domain_meta_dict.items():
            if field_details_dict['type'] == 'DERIVED' or field_details_dict['type'] == 'DOMAIN':
                output_tag = field_details_dict['output']
                logger.info(f"     DERIVING {field_tag}, {field_details_dict}")
                # NB Using an explicit dict here instead of kwargs because this code here
                # doesn't know what the keywords are at 'compile' time.
                args_dict = {}
                for arg_name, field_name in field_details_dict['argument_names'].items():
                    logger.info(f"     -- {field_tag}, arg_name:{arg_name} field_name:{field_name}")
                    if field_name not in output_dict:
                        logger.error(f" domain:{domain} field:{field_tag} could not find {field_name} in {output_dict}")
                    try:
                        args_dict[arg_name] = output_dict[field_name][0]
                    except Exception as x:
                        logger.error(f" arg_name: {arg_name} field_name:{field_name} args_dict:{args_dict} output_dict:{output_dict}")
                try:
                    function_value = (field_details_dict['FUNCTION'](args_dict), 'Derived')
                    if field_details_dict['type'] == 'DOMAIN':
                        domain_id = function_value
                        logger.info(f"     DOMAIN captured as {function_value} for  {field_tag}, {field_details_dict}")
                    else:
                        output_dict[field_tag] = function_value
                        logger.info(f"     DERIV-ed {function_value} for {field_tag}, {field_details_dict} {output_dict[field_tag]}")
                except TypeError as e:
                    logger.error(e)
                    logger.error(f"calling something that isn't a function {field_details_dict['FUNCTION']}. You may have quotes around it in  a python mapping structure if this is a string: {type(field_details_dict['FUNCTION'])}")
   
	# Clean the dict by removing fields with a False output tag  
        clean_output_dict = {}
        for key in output_dict:
       	    field_details_dict = domain_meta_dict[key]
            if field_details_dict['output'] :
                clean_output_dict[key] = output_dict[key]

        # Add a "root" column to show where this came from
        print(f"DOMAIN domain:{domain} root:{domain_meta_dict['root']['element']}   ROOT path:{root_path}")
        clean_output_dict['root_path'] = (root_path, 'root_path')

        output_list.append(clean_output_dict)
    # Check if the domain matches the domain_id that comes up from this concept, drop the row if they don't match.
    if domain_id is None or domain_id[0] == domain:            
        logger.info(f"******* ADDING  {domain_id} {domain}")
        return output_list
    else:
        logger.info(f"******* DROPPING  {domain_id} {domain}")
        return None
  
 
def parse_doc(file_path, metadata):
    """ Parses many domains from a single file, collects them
        into a dict to return.
    """
    omop_dict = {}
    tree = ET.parse(file_path)
    for domain, domain_meta_dict in metadata.items():
        try:
            domain_data_dict =  parse_domain_from_dict(tree, domain, domain_meta_dict)
            omop_dict[domain] = domain_data_dict
        except Exception as e:
            logger.error("parse_doc threw")
            logger.error(e)
    return omop_dict


def print_omop_structure(omop):
    """ prints a dict of parsed domains as returned from parse_doc()
        or parse_domain_from_dict()
    """
    print(f"PK_dict: {PK_dict}")
    for domain, domain_list in omop.items():
        if domain_list is None:
            logger.error(f"no data for domain {domain}")
        else:
            for domain_data_dict in domain_list:
                n=0
                print(f"\n\nDOMAIN: {domain}")
                for field, parts in domain_data_dict.items():
                    print(f"    FIELD:{field}")
                    print(f"        VALUE:{parts[0]}")
                    print(f"        PATH:{parts[1]}")
                    n=n+1
                print(f"\n\nDOMAIN: {domain} {n}\n\n")

if __name__ == '__main__':  
    
    # GET FILE
    file_paths = [ 
        '../resources/CCDA_CCD_b1_Ambulatory_v2.xml',
#        '../resources/CCDA_CCD_b1_InPatient_v2.xml',
#        '../resources/170.314b2_AmbulatoryToC.xml',  # has errors and uses different OIDs
#         '../resources/ToC_CCDA_CCD_CompGuideSample_FullXML.xml',
##        '../resources/Manifest_Medex/bennis_shauna_ccda.xml.txt',
##        '../resources/Manifest_Medex/eHX_Terry.xml.txt',
#        '../resources/CRISP Content Testing Samples/CRISP Main Node/anna_flux.xml',
#        '../resources/CRISP Content Testing Samples/HealtheConnect Alaska/healtheconnectak-ccd-20210226.2.xml'
    ]
    if False: # for getting them on the Foundry
        from foundry.transforms import Dataset
        ccd_ambulatory = Dataset.get("ccda_ccd_b1_ambulatory_v2")
        ccd_ambulatory_files = ccd_ambulatory.files().download()
        ccd_ambulatory_path = ccd_ambulatory_files['CCDA_CCD_b1_Ambulatory_v2.xml']

    for filepath in file_paths: 
        data = parse_doc(filepath, get_meta_dict()) 
        print_omop_structure(data) 
