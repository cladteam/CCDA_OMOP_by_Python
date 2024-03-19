#!/usr/bin/env python3

# observation.py
#
# collects OMOP patient observation (TBD) attributes from CCDA patientRole
# depends: patient.py must be run on this document beforehand so the ID has been entered.
# CCDA document: CCD

import json
from vocab_map_file import vocab_map
import id_map


def create():
    dest = { 'observation_id': None, 'person_id': None, 'observation_concept_id': None, 'observation_date': None, 
             'value_as_number': None, 'value_as_string': None, 'value_as_concept_id': None, 
             'provider_id': None}
    return dest
    
def convert():
    obs_vocabulary_id = data['code']['coding'][0]['system']
    obs_concept_code  = data['code']['coding'][0]['code']
    (concept_name, obs_concept_id, vocab) = vocab_map[(obs_vocabulary_id, obs_concept_code)]

    dest = create()

    dest['observation_id']         = data['id']
    dest['.person_id']              = data['subject']['reference']
    dest['observation_concept_id'] = obs_concept_id
    dest['observation_date']       = data['effectiveDateTime']
    dest['value_as_number']        = 0
    dest['value_as_string']        = 0
    dest['value_as_concept_id']    = 0
    dest['provider_id']            = data['performer'][0]['reference']

    return dest




