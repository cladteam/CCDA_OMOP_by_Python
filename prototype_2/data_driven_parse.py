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

logger = logging.getLogger('basic logging')
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

ns = {
   '': 'urn:hl7-org:v3',  # default namespace
   'hl7': 'urn:hl7-org:v3',
   'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
   'sdtc': 'urn:hl7-org:sdtc'
}



""" The meatadata is 3 nested dictionaries: 
    - meta_dict: the dict of all domains
    - domain_dict: a dict describing a particular domain
    - field_dict: a dict describing a field component of a domain
    These names are used in the code to help orient the reader
    
    An output_dict is created for each domain. The keys are the field names,
    and the values are the values of the attributes from the elements.
"""    

PK_dict = {}

def map_oid(vocabuarly_oid):
    """ maps an OID used in CCDA to indicate the vocabulary
        to an OMOP vocabulary_id.
        FIX: needs the data, needs written
    """
    return 1
    
def map_to_omop_concept_id(vocabulary_id, concept_code):
    """ Simply maps vocabulary_id, concept_code to an OMOP concept_id.
        FIX: needs the data, needs written.
        Somewhat redundant in that map_to_standard_omop_concept_id
        is much more useful and likely to be used.
    """
    return 2

def map_to_standard_omop_concept_id(vocabulary_id, concept_code):
    """ Maps vocabulary_id, concept_code to a standard OMOP 
        concept_id by joining through concept_relationship.
        FIX: needs the data, needs written
    """
    return 2

def map_hl7_to_omop(vocabulary_oid, concept_code):
    """ This would map an HL7 vocabulary_oid to an OMOP vocabulary_id,
        then map both vocabulary_id and concept_code to an OMOP concept_id
    """
    #vocabulary_id = map_oid(args_dict['vocabulary_oid'])
    #concept_id = map_to_standard_omop_concept_id(vocabulary_id, args_dict['concept_code'])
    #return concept_id
    return 123456

def map_hl7_to_omop_w_dict_args(args_dict):
    """ expects: vocabulary_oid, concept_code
        FIX: needs the data, needs written
        FIX: consider kwargs and the pythonic way of doing this!
    """
    return map_hl7_to_omop(args_dict['vocabulary_oid'], args_dict['concept_code'])

meta_dict = {
    # domain : { field : [ element, attribute, value_transformation_function ] }
    'person': { 
        'root' : { # name is required to be root, see foolish consistency below
            'type' : 'ROOT', # (really a foolish consistency, not used here)
            'element': "./recordTarget/patientRole"
        }, 
        # [@root='2.16.840.1.113883.4.1']
        'person_other_id' : {
            'type' : 'FIELD',
            'element' : 'id[@root="2.16.840.1.113883.4.6"]',
            'attribute' : "extension"
        },
        'person_id' : {
            'type' : 'PK',
            'element' : 'id[@root="2.16.840.1.113883.4.1"]',
            'attribute' : "extension"
        },
        'gender_concept_code' : { 
            'type' : 'FIELD',
            'element' : "patient/administrativeGenderCode", 
            'attribute' : "code"
        },
        'gender_concept_codeSystem' : { 
            'type' : 'FIELD',
            'element' : "patient/administrativeGenderCode", 
            'attribute' : "code"
        },
        'gender_concept_id' : {
            'type' : 'DERIVED',
            'FUNCTION' : map_hl7_to_omop_w_dict_args, 
            'argument_names' : { 
                'concept_code' : 'gender_concept_code', 
                'vocabulary_oid' : 'gender_concept_codeSystem'
            }
        },
        'date_of_birth': { 
            'type' : 'FIELD',
            'element' : "patient/birthTime", 
            'attribute' : "value" 
        },
        'race_concept_code' : { 
            'type' : 'FIELD',
            'element' : "patient/raceCode", 
            'attribute' : "code"
        },
        'race_concept_codeSystem' : { 
            'type' : 'FIELD',
            'element' : "patient/raceCode", 
            'attribute' : "codeSystem"
        },
        'race_concept_id':{
            'type' : 'DERIVED',
            'FUNCTION' : map_hl7_to_omop_w_dict_args,
            'argument_names' : { 
                'concept_code' : 'race_concept_code', 
                'vocabulary_oid' : 'race_concept_codeSystem'
            }
        },
        'ethnicity_concept_code' : {
            'type' : 'FIELD',
            'element' : "patient/ethnicGroupCode", 
            'attribute': "code"
        },
        'ethnicity_concept_codeSystem' : {
            'type' : 'FIELD',
            'element' : "patient/ethnicGroupCode", 
            'attribute': "codeSystem"
        },
        'ethnicity_concept_id' : {
            'type' : 'DERIVED',
            'FUNCTION' : map_hl7_to_omop_w_dict_args, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'ethnicity_concept_code', 
                'vocabulary_oid' : 'ethnicity_concept_codeSystem'
            }
        },
    },

    'encounter' : {
        'root': {
            'type' : 'ROOT',
            'element' : "./componentOf/encompassingEncounter"
        }, # FIX [@root='2.16.840.1.113883.4.6']
        'person_id' : { 
            'type' : 'FK',
            'FK' : 'person_id' 
        },
        'visit_occurrence_id' : { 
            'type' : 'PK',
            'element' : "id",
            'attribute': "root",
        },
        'visit_concept_code' : { 
            'type' : 'FIELD',
            'element' : "code",   # FIX ToDo is this what I think it is?,
            'attribute' : "code"
        }, 
        'visit_concept_codeSystem' : { 
            'type' : 'FIELD',
            'element' : "code",
            'attribute' : "codeSystem"
        }, 
        'visit_concept_id' : {
            'type' : 'DERIVED',
            'FUNCTION' : map_hl7_to_omop_w_dict_args, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'visit_concept_code', 
                'vocabulary_oid' : 'visit_concept_codeSystem'
            }
        },
        'care_site_id' : { 
            'type' : 'FIELD',
            'element' : "location/healthCareFacility/id",
            'attribute' : "root"
        },
        # FIX is it consistenly a high/low pair? do we sometimes get just effectiveTime@value ?
        'start_time' : { 
            'type' : 'FIELD',
            'element' : "effectiveTime/low",
            'attribute' : "value"
        },
        'end_time' :  { 
            'type' : 'FIELD',
            'element' : "effectiveTime/high",
            'attribute' : "value"
        }
    },

    'observation' : {
        'root' : {
            'type' : 'ROOT',
            'element': 
                  ("./component/structuredBody/component/section/"
                   "templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']"
                   "/../entry/organizer/component/observation")
                 },
        'person_id' : { 
            'type' : 'FK', 
            'FK' : 'person_id' 
        }, 
        'visit_occurrence_id' :  { 
            'type' : 'FK', 
            'FK' : 'visit_occurrence_id' 
        }, 
        'observation_id' : {  # FIX, these IDs come up the same for all 3 observations in the CCD Ambulatory doc.
            'type' : 'FIELD',
            'element' : 'id',
            'attribute' : 'root'   ### FIX ????
        },
        'observation_concept_code' : {
            'type' : 'FIELD',
            'element' : "code" ,
            'attribute' : "code"
        },
        'observation_concept_codeSystem' : {
            'type' : 'FIELD',
            'element' : "code",
            'attribute' : "codeSystem"
        },
        'observation_concept_id' : {
            'type' : 'DERIVED', 
            'FUNCTION' : map_hl7_to_omop_w_dict_args, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'observation_concept_code', 
                'vocabulary_oid' : 'observation_concept_codeSystem'
            }
        },
        'observation_concept_displayName' : {
            'type' : 'FIELD',
            'element' : "code",
            'attribute' : "displayName"
        },
        # FIX same issue as above. Is it always just a single value, or do we ever get high and low?
        'time' : {
            'type' : 'FIELD',
            'element' : "effectiveTime",
            'attribute' : "value"
        },
        'value' : { 
            'type' : 'FIELD',
            'element' : "value" ,
            'attribute' : "value"
        },
        'value_type' : { 
            'type' : 'FIELD',
            'element' : "value",
            'attribute' : "type"
        },
        'value_unit' :  { 
            'type' : 'FIELD',
            'element' : 'value',
            'attribute' : 'unit'
        }
    }
}


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
                args_dict = {}
                for arg_name, field_name in field_details_dict['argument_names'].items():
                    logger.info(f"     -- {field_tag}, arg_name:{arg_name} field_name:{field_name}")
                    args_dict[arg_name] = output_dict[field_name]
                output_dict[field_tag] = field_details_dict['FUNCTION'](args_dict)
                logger.info(f"     DERIV-ed {field_tag}, {field_details_dict} {output_dict[field_tag]}")

        output_list.append(output_dict)
        
    return output_list
  
 
def parse_doc(file_path):
    """ Parses many domains from a single file, collects them
        into a dict to return.
    """
    omop_dict = {}
    tree = ET.parse(file_path)
    for domain, domain_meta_dict in meta_dict.items():
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
 
    data = parse_doc(ccd_ambulatory_path) 
    print_omop_structure(data) 
