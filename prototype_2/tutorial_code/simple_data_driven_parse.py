#!/usr/bin/env python3

""" Simple Table-Driven ElementTree parsing in Python

    As a version of the main data-driven parsing work, this one doesn't do concept
    mapping or PKs and FKs or other derived values in order not to crowd the a presentation of the basic idea.

 This version puts the paths into a data structure and explores using
 one function driven by the data. 
 - The mapping_dict is hard-coded here. An next step would be to read that in from a file.
 - Value transformation is stubbed out waiting for vocabularies to be loaded, and to 
   figure out how to use them once there.
 
 Chris Roeder
 2024-07-25: needs access to vocabulary, needs to do multiple obsrvations, needs dervied values from code on foundry
 2024-07-26: needs test driver, the main function needs broken out into file, field and attribute
"""

import pandas as pd
# mamba install -y -q lxml
import lxml
from lxml import etree as ET
import logging

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



meta_dict = {
    # domain : { field : [ element, attribute ] }
    'person': { 
        'root' : {'element': "./recordTarget/patientRole"} , # root is a special case, no attribute here, and the name matters
        # [@root='2.16.840.1.113883.4.1']
        'person_other_id' : {
            'element' : 'id[@root="2.16.840.1.113883.4.6"]',
            'attribute' : "extension"
        },
        'person_id' : {
            'element' : 'id[@root="2.16.840.1.113883.4.1"]',
            'attribute' : "extension"
        },
        'gender_concept_code' : { 
            'element' : "patient/administrativeGenderCode", 
            'attribute' : "code"
        },
        'gender_concept_codeSystem' : { 
            'element' : "patient/administrativeGenderCode", 
            'attribute' : "code"
        },
        'date_of_birth': { 
            'element' : "patient/birthTime", 
            'attribute' : "value" 
        },
        'race_concept_code' : { 
            'element' : "patient/raceCode", 
            'attribute' : "code"
        },
        'race_concept_codeSystem' : { 
            'element' : "patient/raceCode", 
            'attribute' : "codeSystem"
        },
        'ethnicity_concept_code' : {
            'element' : "patient/ethnicGroupCode", 
            'attribute': "code"
        },
        'ethnicity_concept_codeSystem' : {
            'element' : "patient/ethnicGroupCode", 
            'attribute': "codeSystem"
        },
    },
    'encounter' : {
        'root': {'element' : "./componentOf/encompassingEncounter"}, # FIX [@root='2.16.840.1.113883.4.6']
        'visit_occurrence_id' : { 
            'element' : "id",
            'attribute': "root",
        },
        'visit_concept_code' : { 
            'element' : "code",   # FIX ToDo is this what I think it is?,
            'attribute' : "code"
        }, 
        'visit_concept_codeSystem' : { 
            'element' : "code",
            'attribute' : "codeSystem"
        }, 
        'care_site_id' : { 
            'element' : "location/healthCareFacility/id",
            'attribute' : "root"
        },
        # FIX is it consistenly a high/low pair? do we sometimes get just effectiveTime@value ?
        'start_time' : { 
            'element' : "effectiveTime/low",
            'attribute' : "value"
        },
        'end_time' :  { 
            'element' : "effectiveTime/high",
            'attribute' : "value"
        }
    },
    'observation' : {
        'root' : {'element': 
                  ("./component/structuredBody/component/section/"
                   "templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']"
                   "/../entry/organizer/component/observation")
                 },
        'observation_id' : {  # FIX, these IDs come up the same for all 3 observations in the CCD Ambulatory doc.
            'element' : 'id',
            'attribute' : 'root'   ### FIX ????
        },
        'observation_concept_code' : {
            'element' : "code" ,
            'attribute' : "code"
        },
        'observation_concept_codeSystem' : {
            'element' : "code",
            'attribute' : "codeSystem"
        },
        'observation_concept_displayName' : {
            'element' : "code",
            'attribute' : "displayName"
        },
        # FIX same issue as above. Is it always just a single value, or do we ever get high and low?
        'time' : {
            'element' : "effectiveTime",
            'attribute' : "value"
        },
        'value' : { 
            'element' : "value" ,
            'attribute' : "value"
        },
        'value_type' : { 
            'element' : "value",
            'attribute' : "type"
        },
        'value_unit' :  { 
            'element' : 'value',
            'attribute' : 'unit'
        }
    }
}


def parse_field_from_dict(field_details_dict, domain_root_element, domain, field_tag):
    if 'element' not in field_details_dict:
        logging.error(f"could find key 'element' in the field_details_dict: {field_details_dict}")
    else:
        logging.info(f"FIELD {field_details_dict['element']} for {domain}/{field_tag}")
        field_element = domain_root_element.find(field_details_dict['element'], ns)
        if field_element is None:
            logging.error(f"could find field element {field_details_dict['element']} for {domain}/{field_tag}")
            return None
    
        if 'attribute' not in field_details_dict:
            logging.error(f"could not find key 'attribute' in the field_details_dict: {field_details_dict}")
        else: 
            logging.info(f"ATTRIBUTE   {field_details_dict['attribute']} for {domain}/{field_tag} {field_details_dict['element']} ")
            attribute_value = field_element.get(field_details_dict['attribute'])
            if attribute_value is None:
                logging.warning(f"no value for field element {field_details_dict['element']} for {domain}/{field_tag}")
            return(attribute_value)
    

def parse_domain_from_dict(tree, domain, domain_meta_dict):
    """ The main logic is here. 
        Given a tree from ElementTree representing a CCDA document (ClinicalDocument, not just file),
        parse the different domains out of it.
        Return a dictionary, keyed by domain name, of dictionaries of rows, each a dictionary
        of fields.

        Returns None on error.
    """
    # Find root
    if 'root' not in domain_meta_dict:
        logging.error(f"DOMAIN domain:{domain} has not 'root' element.")
        return None

    logging.info(f"DOMAIN domain:{domain} root:{domain_meta_dict['root']['element']}")
    domain_root_element_list = tree.findall(domain_meta_dict['root']['element'], ns)
    if domain_root_element_list is None or len(domain_root_element_list) == 0: 
        logging.error(f"couldn't find root element for {domain} with {domain_meta_dict['root']['element']}")
        return None
    
    # Do others. 
    # Watch for two kinds of errors: 
    #  1) the metadata has mis-spelled keys (Python domain)
    #  2) the paths that appear as values to 'element' keys don't work in ElementTree (XPath domain)
    output_list = []
    for domain_root_element in domain_root_element_list:
        output_dict = {}
        logging.info(f"ROOT for domain:{domain}, we have tag:{domain_root_element.tag} attributes:{domain_root_element.attrib}")
        for (field_tag, field_details_dict) in domain_meta_dict.items():
            if field_tag != 'root':  # FIX do the pythonic filter on the list with a lambda?
               field_value = parse_field_from_dict(field_details_dict, domain_root_element, domain, field_tag) 
               output_dict[field_tag] = field_value
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
