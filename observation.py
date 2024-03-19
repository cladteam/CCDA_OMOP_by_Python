#!/usr/bin/env python3

# observation.py
#
# collects OMOP patient observation (TBD) attributes from CCDA patientRole
# depends: patient.py must be run on this document beforehand so the ID has been entered.
# CCDA document: CCD

# ToDo
# - person_id
# - provider_id
# - value type

import json
import vocab_map_file
import id_map


def create():
    dest = { 'observation_id': None, 'person_id': None, 'observation_concept_id': None, 'observation_date': None, 
             'value_as_number': None, 'value_as_string': None, 'value_as_concept_id': None, 
             'provider_id': None}
    return dest
    
def convert(tree):
    child_list = tree.findall(".")
    child = child_list[0]


    ##documentationOf = child.findall("./{urn:hl7-org:v3}documentationOf") # clinician, Dx
    ##componentOf = child.findall("./{urn:hl7-org:v3}componentOf") # encompassing encounter, visit

    results_section  = child.findall("./{urn:hl7-org:v3}component/" + 
                                     "{urn:hl7-org:v3}structuredBody/" + 
                                     "{urn:hl7-org:v3}component/" +
                                     "{urn:hl7-org:v3}section/" + 
                                     "{urn:hl7-org:v3}templateId[@root='" + vocab_map_file.results + "']/..")

    print("sections:", len(results_section), results_section[0].tag)

    results_observations = results_section[0].findall("{urn:hl7-org:v3}entry/" +
                                                      "{urn:hl7-org:v3}organizer/" +
                                                      "{urn:hl7-org:v3}component/" +
                                                      "{urn:hl7-org:v3}observation")

    dest = list(range(len(results_observations)))
    i=0
    for obs in results_observations:
        observation_id = obs.find("{urn:hl7-org:v3}id")
        observation_code = obs.find("{urn:hl7-org:v3}code")
        observation_code_code = observation_code.attrib['code'] 
        observation_code_codeSystem = observation_code.attrib['codeSystem'] 
        observation_concept_id = (concept_name, obs_concept_id, vocab, code) = vocab_map_file.vocab_map[(observation_code_codeSystem, observation_code_code)]
        observation_date= obs.find("{urn:hl7-org:v3}effectiveTime")
        observation_value = obs.find("{urn:hl7-org:v3}value")
        observation_value_value = observation_value.attrib['value']
        ##observation_value_type = observation_value.attrib['xsi:type']
        observation_value_type = observation_value.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']
        observation_value_unit = observation_value.attrib['unit']
   
        dest[i] = create()
        dest[i]['observation_id']         =  observation_id
        dest[i]['person_id']              = None  ##########################
        dest[i]['observation_concept_id'] =  observation_concept_id
        dest[i]['observation_date']       =  observation_date
        # observation value TYPE???  #############
        if observation_value_type == 'PQ':
            dest[i]['value_as_number']        =  observation_value_value
        else:
            dest[i]['value_as_number']        =  None
            dest[i]['value_as_string']        =  observation_value
        dest[i]['value_as_concept_id']    =  None
        dest[i]['provider_id']            =  None #########################
        i+=1

    return dest

    # child {urn:hl7-org:v3}templateId
    # child {urn:hl7-org:v3}referenceRange (low, high)
    # child {urn:hl7-org:v3}text
    # child {urn:hl7-org:v3}statusCode
    # child {urn:hl7-org:v3}interpretationCode
    # child {urn:hl7-org:v3}methodCode
    # child {urn:hl7-org:v3}targetSiteCode
    # child {urn:hl7-org:v3}author
    
    
    
#    obs_vocabulary_id = data['code']['coding'][0]['system']
#    obs_concept_code  = data['code']['coding'][0]['code']
#





