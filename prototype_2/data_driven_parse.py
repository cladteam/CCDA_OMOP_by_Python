#!/usr/bin/env python3

""" Table-Driven ElementTree parsing in Python

 This version puts the paths into a data structure and explores using
 one function driven by the data. 
 - The mapping_dict is hard-coded here. An next step would be to read that in from a file.
 - Value transformation is stubbed out waiting for vocabularies to be loaded, and to 
   figure out how to use them once there.
 
 Chris Roeder
 2024-07-25: needs access to vocabulary, needs to do multiple obsrvations, needs dervied values from code on foundry
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

PK_dict = {}

def map_oid(vocabuarly_oid):
    return 1
    
def map_to_omop_concept_id(vocabulary_id, concept_code):
    return 2

def map_hl7_to_omop(args_dict):
    # expects: vocabulary_oid, concept_code
        # This would map an HL7 vocabulary_oid to an OMOP vocabulary_id,
        # then map both vocabulary_id and concept_code to an OMOP concept_id
        # STUB
        #vocabulary_id = map_oid(args_dict['vocabulary_oid'])
        #concept_id = map_to_omop_concept_id(vocabulary_id, args_dict['concept_code'])
        #return concept_id
        return 123456

meta_dict = {
    # domain : { field : [ element, attribute ] }
    # evo: domain : { field : [ element, attribute, value_transformation_function ] }
    'person': { 
        'root' : {'element': "./recordTarget/patientRole"} , # root is a special case, no attribute here, and the name matters
        # [@root='2.16.840.1.113883.4.1']
        'person_other_id' : {
            'element' : 'id[@root="2.16.840.1.113883.4.6"]',
            'attribute' : "extension"
        },
        'person_id' : {
            'PK' : "",  # used as a tag to mark this field for addition to the PK_dict 
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
        'gender_concept_id' : {
            'DERIVED': "", # marks as derived from other fields
            'FUNCTION' : map_hl7_to_omop, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'gender_concept_code', 
                'vocabulary_oid' : 'gender_concept_codeSystem'
            }
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
        'race_concept_id':{
            'DERIVED': "", # marks as derived from other fields
            'FUNCTION' : map_hl7_to_omop, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'race_concept_code', 
                'vocabulary_oid' : 'race_concept_codeSystem'
            }
        },
        'ethnicity_concept_code' : {
            'element' : "patient/ethnicGroupCode", 
            'attribute': "code"
        },
        'ethnicity_concept_codeSystem' : {
            'element' : "patient/ethnicGroupCode", 
            'attribute': "codeSystem"
        },
        'ethnicity_concept_id' : {
            'DERIVED': "", # marks as derived from other fields
            'FUNCTION' : map_hl7_to_omop, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'ethnicity_concept_code', 
                'vocabulary_oid' : 'ethnicity_concept_codeSystem'
            }
        },
    },
    'encounter' : {
        'root': {'element' : "./componentOf/encompassingEncounter"}, # FIX [@root='2.16.840.1.113883.4.6']
        'person_id' : { 'FK' : 'person_id' }, # FK here means to get the value from the PK_dict instead of the XML tree.
        'visit_occurrence_id' : { 
            'element' : "id",
            'attribute': "root",
            'PK' : "" # used as a tag to mark this field for addition to the PK_dict 
        },
        'visit_concept_code' : { 
            'element' : "code",   # FIX ToDo is this what I think it is?,
            'attribute' : "code"
        }, 
        'visit_concept_codeSystem' : { 
            'element' : "code",
            'attribute' : "codeSystem"
        }, 
        'visit_concept_id' : {
            'DERIVED': "", # marks as derived from other fields
            'FUNCTION' : map_hl7_to_omop, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'visit_concept_code', 
                'vocabulary_oid' : 'visit_concept_codeSystem'
            }
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
        'person_id' : { 'FK' : 'person_id' }, # FK here means to get the value from the PK_dict instead of the XML tree.
        'visit_occurrence_id' :  { 'FK' : 'visit_occurrence_id' }, # FK here means to get the value from the PK_dict instead of the XML tree.
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
        'observation_concept_id' : {
            'DERIVED': "", # marks as derived from other fields
            'FUNCTION' : map_hl7_to_omop, # not the string representing the name, but function itself in Python space.
            'argument_names' : { 
                'concept_code' : 'observation_concept_code', 
                'vocabulary_oid' : 'observation_concept_codeSystem'
            }
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

    
def parse_domain_from_dict(tree, domain, domain_meta_dict):

    # Find root
    logging.info(f"DOMAIN domain:{domain} root:{domain_meta_dict['root']['element']}")
    root_element_list = tree.findall(domain_meta_dict['root']['element'], ns)
    if root_element_list is None or len(root_element_list) == 0: 
        logging.error(f"couldn't find root element for {domain} with {domain_meta_dict['root']['element']}")
        return None
    
    # Do others. 
    # Watch for two kinds of errors: 
    #  1) the metadata has mis-spelled keys (Python domain)
    #  2) the paths that appear as values to 'element' keys don't work in ElementTree (XPath domain)
    output_list = []
    print(f"NUM ROOTS {len(root_element_list)}")
    for root_element in root_element_list:
        output_dict = {}
        logging.info(f"ROOT for domain:{domain}, we have tag:{root_element.tag} attributes:{root_element.attrib}")
        for (field_tag, field_details_dict) in domain_meta_dict.items():
            if field_tag != 'root' and 'DERIVED' not in field_details_dict:
                if 'FK' in field_details_dict:
                     logging.info(f"FIELD FK for {domain}/{field_tag}")
                     if field_tag in PK_dict:
                        output_dict[field_tag] = PK_dict[field_tag] 
                     else:
                        logging.error(f"could not find {field_tag}  in PK_dict for {domain}/{field_tag}")
                        output_dict[field_tag] = None
                elif 'element' not in field_details_dict:
                    logging.error(f"could find key 'element' in the field_details_dict: {field_details_dict}")
                else:
                    logging.info(f"FIELD non-FK {field_details_dict['element']} for {domain}/{field_tag}")
                    field_element = root_element.find(field_details_dict['element'], ns)
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
                        output_dict[field_tag] = attribute_value
                        if 'PK' in field_details_dict:
                            PK_dict[field_tag] = attribute_value

        # Do derived values now that their inputs should be available in the output_dict                           
        for (field_tag, field_details_dict) in domain_meta_dict.items():
            if field_tag != 'root' and 'DERIVED' in field_details_dict:
                logging.info(f" DERIVING {field_tag}, {field_details_dict}")
                args_dict = {}
                for arg_name, field_name in field_details_dict['argument_names'].items():
                    args_dict[arg_name] = output_dict[field_name] 
                output_dict[field_tag] = field_details_dict['FUNCTION'](args_dict)

        output_list.append(output_dict)
        
    return output_list
  
 
def parse_doc(file_path):
    omop_dict = {}
    tree = ET.parse(file_path)
    for domain, domain_meta_dict in meta_dict.items():
        domain_data_dict =  parse_domain_from_dict(tree, domain, domain_meta_dict)
        omop_dict[domain] = domain_data_dict
    return omop_dict


def print_omop_structure(omop):
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
