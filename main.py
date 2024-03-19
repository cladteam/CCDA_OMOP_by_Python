#!/usr/bin/env python3

import xml.etree.ElementTree as ET
from pathlib import Path
import id_map

import location
import person
import observation
import util


tree = ET.parse("resources/CCDA_CCD_b1_InPatient_v2.xml")
loc =  location.convert(tree)

if util.check_CCD_document_type(tree):
    target = {  
            'location': loc,
            'person': person.convert(tree) ,
            'observation': observation.convert(tree) 
             }
    print("====================================")
    print(target['location'])
    print(target['person'])
    for obs in target['observation']:
        print(obs)
else:
    print("wrong doc type boss")


