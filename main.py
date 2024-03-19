#!/usr/bin/env python3

import xml.etree.ElementTree as ET
from pathlib import Path
import id_map

import location
import person
import observation
import util

input_file_list = [ 'resources/CCDA_CCD_b1_InPatient_v2.xml',
                    'resources/CCDA_CCD_b1_Ambulatory_v2.xml',
                    'resources/Inpatient_Encounter_Discharged_to_Rehab_Location(C-CDA2.1).xml',
                    'resources/170.314b2_AmbulatoryToC.xml',
                    'resources/ToC_CCDA_CCD_CompGuideSample_FullXML.xml' ]

for input_file in input_file_list:
    tree = ET.parse(input_file)
    loc =  location.convert(tree)

    print(f"==================================== {input_file} ===")
    if util.check_CCD_document_type(tree):
        target = {  
             'location': loc,
             'person': person.convert(tree) ,
             'observation': observation.convert(tree) 
               }
        print(target['location'])
        print(target['person'])
        for obs in target['observation']:
            print(obs)
    else:
        print(f"wrong doc type boss {input_file}")


