#!/usr/bin/env python3

import xml.etree.ElementTree as ET
from pathlib import Path
import id_map

import location
import person
import observation

tree = ET.parse("resources/CCDA_CCD_b1_InPatient_v2.xml")
loc =  location.convert(tree)
##id_map.dump()
target = {  
            'location': loc,
            'person': person.convert(tree) #,
            #'observation': observation.convert(tree) 
         }
print(target)


