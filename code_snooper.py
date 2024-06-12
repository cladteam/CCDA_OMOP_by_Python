#!/usr/bin/env python3

"""
    A basic example for parsing an XML file, fetching some attribute values,
    and converting them to an OMOP concept_id.
    Uses some support for keeping the OMOP concept table in Spark.
    
    This version uses iterparse, which is great for building paths from 
    the events, though you have to filter the nodes yourself. Done here with re.
"""

import xml.etree.ElementTree as ET # https://docs.python.org/3/library/xml.etree.elementtree.html
import re # https://docs.python.org/3.9/library/re.html
from util.xml_ns import ns
from util.vocab_map_file import  oid_map

INPUT_FILENAME = 'resources/CCDA_CCD_b1_InPatient_v2.xml'
tree = ET.parse(INPUT_FILENAME)

# credit: https://stackoverflow.com/questions/68215347/capture-all-xml-element-paths-using-xml-etree-elementtree
def pathGen(fn):
    path = []
    it = ET.iterparse(fn, events=('start', 'end'))
    for evt, el in it:
        if evt == 'start':
            # trim off namespace stuff in curly braces
            trimmed_tag = re.sub(r"{.*}",'', el.tag)

            # don't include the "/ClinicalDocument" part at the very start
            if trimmed_tag == 'ClinicalDocument':
                path.append(".")
            else:
                path.append(trimmed_tag)

            yield '/'.join(path)
        else:
            path.pop()

for path in pathGen(INPUT_FILENAME):
    # just get the paths that end with a code
    if re.fullmatch(r".*/code", path):
        try:
            for code in tree.findall(path, ns):
                vocabulary_oid = code.attrib['codeSystem']
                vocabulary_id = oid_map[vocabulary_oid][0]
                concept_code = code.attrib['code']
                print(f"{path}  {vocabulary_id} {concept_code}")
        except:
            print(f"{path}  -- no attributes, or not both --")

