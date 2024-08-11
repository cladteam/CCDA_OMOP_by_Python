#!/usr/bin/env python3

# Plain Python ElementTree parsing
# Chris Roeder
# 2024-07-24: functions should be called parse_and_convert_X(), needs access to vocabulary
# 2024-07-25: as an alternate to data_driven_parse.py, this one does not deal with PK-FKs.


import pandas as pd
# mamba install -y -q lxml
import lxml
from lxml import etree as ET
import logging


logger = logging.getLogger(__name__)


ns = {
   '': 'urn:hl7-org:v3',  # default namespace
   'hl7': 'urn:hl7-org:v3',
   'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
   'sdtc': 'urn:hl7-org:sdtc'
}

def parse_patient(tree):
    root = "./recordTarget/patientRole/"
    id = None
    id_element = tree.find(root + "id[@root='2.16.840.1.113883.4.1']", ns)
    if id_element is not None:
        id = id_element.get('extension')
    else:
        id_element = tree.find(root + "id", ns)
        if id_element is not None:
            id = id_element.get('extension')

    person_dict = {
            "person_id": id,
            "gender_test": tree.find(root + "patient/administrativeGenderCode", ns).get("code"),   # noqa: E501
            "date_of_birth": tree.find(root + "patient/birthTime", ns).get("value"),  # noqa: E501
            "race" : tree.find(root + "patient/raceCode", ns).get("code"),
            "ethnicity" : tree.find(root + "patient/ethnicGroupCode", ns).get("code")  # noqa: E501
            }
    return person_dict


def parse_encounter(tree, person_id):
    root = "./componentOf/encompassingEncounter/"
    id = None
    id_element = tree.find(root + "id[@root='2.16.840.1.113883.4.6']", ns)
    if id_element is not None:
        id = id_element.get('extension')
    else:
        id_element = tree.find(root + "id", ns)
        if id_element is not None:
            id = id_element.get('extension')

    encounter_dict = {
            "person_id": person_id,
            "visit_occurrence_id" : id,
            "visit_concept_code": tree.find(root + "code", ns).get("code"),   # ToDo is this what I think it is?
            "visit_concept_codeSystem": tree.find(root + "code", ns).get("codeSystem"),
            "care_site_id": tree.find(root + "location/healthCareFacility/id", ns).get("root"), 
            "start_time" : tree.find(root + "effectiveTime/low", ns).get("value"),
            "end_time" : tree.find(root + "effectiveTime/high", ns).get("value")
            }
    return encounter_dict


def parse_results_observation(tree, person_id, visit_occurrence_id):
    """ This parses observations from the RESULTS section.
        FIX: Observations are also found in SOCIAL HISTORY, VITAL SIGNS,  possibly others.
        It's possible to strip the templateId from this path and pick them all up, but
        not forgetting domain_id routing, putting them in appropriate OMOP tables may be 
        facillitated with the source section.
    """
    root = "./component/structuredBody/component/section/templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']/../entry/organizer/component/observation"
    observation_element_list = tree.findall(root, ns)
    observation_list = list()
    
    for obs in observation_element_list:
        id = None
        id_element = obs.find("id", ns)
        if id_element is not None:
            id = id_element.get('root')

        observation_dict = {
            "person_id": person_id,
            "visit_occurrence_id" : visit_occurrence_id,
            "observation_id": id,  # FIX, these IDs come up the same for all 3 observations in the CCD Ambulatory doc.
            "observation_concept_code": obs.find("./code", ns).get("code"),   
            "observation_concept_codeSystem": obs.find("./code", ns).get("codeSystem"),
            "observation_concept_displayName" : obs.find("./code", ns).get("displayName"),
            "time" : obs.find("./effectiveTime", ns).get("value"),
            "value" : obs.find("./value", ns).get("value"),
            "value_type" : obs.find("./value", ns).get("type"),
            "value_unit" : obs.find("./value", ns).get("unit")
             # visit_concept_id 
        }
        observation_list.append(observation_dict)
    return observation_list
    

if __name__ == '__main__':  
    
    # GET FILE
    ccd_ambulatory_path = '../resources/CCDA_CCD_b1_Ambulatory_v2.xml'
    if False:
        from foundry.transforms import Dataset
        ccd_ambulatory = Dataset.get("ccda_ccd_b1_ambulatory_v2")
        ccd_ambulatory_files = ccd_ambulatory.files().download()
        ccd_ambulatory_path = ccd_ambulatory_files['CCDA_CCD_b1_Ambulatory_v2.xml']
    
    # PARSE                                               
    tree = ET.parse(ccd_ambulatory_path)

    # MINE and print                                                   
    print("PERSON")
    person_dict = parse_patient(tree)
    person_id = person_dict['person_id']
    print(person_dict) 

    print("ENCOUNTER")
    visit_dict = parse_encounter(tree, person_id)
    visit_occurrence_id = visit_dict['visit_occurrence_id']
    print(visit_dict)
    
    print("OBSERVATION")
    observation_list = parse_results_observation(tree, person_id, visit_occurrence_id)
    for obs_dict in observation_list:
        print(obs_dict)
