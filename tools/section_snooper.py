
"""
    CodeSnooper - looks for code elements, fetches their code and codeSystem
                  attributes, mapping OIDs to vocabularies and
                  concept codes to names, lists the paths to the elements
                  with their attributes.
"""

import argparse
import xml.etree.ElementTree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from util.xml_ns import ns
from util.vocab_map_file import oid_map
from util import spark_util
from util.vocab_spark import VocabSpark

INPUT_FILENAME = 'resources/CCDA_CCD_b1_InPatient_v2.xml'
spark_util_object = spark_util.SparkUtil()
spark = spark_util_object.get_spark()

parser = argparse.ArgumentParser(
    prog='CCDA - OMOP Code Snooper',
    description="finds all code elements and shows what concepts the represent",
    epilog='epilog?')
parser.add_argument('-f', '--filename', default=INPUT_FILENAME,
                    help="filename to parse")
args = parser.parse_args()

tree = ET.parse(INPUT_FILENAME)

SECTION_PATH = "./component/structuredBody/component/section"
SECTION_CODE = "./code"
#OBSERVATION_SECTION_CODE = "./code[@code=\"30954-2\"]"
OBSERVATION_PATH = "./entry/organizer/component/observation"

section_elements = tree.findall(SECTION_PATH, ns)
print("\n\n")

for section_element in section_elements:

    section_type=''
    #for section_code_element in section_element.findall(OBSERVATION_SECTION_CODE, ns):
    for section_code_element in section_element.findall(SECTION_CODE, ns):
        if 'displayName' in section_code_element.attrib:
            section_type = section_code_element.attrib['displayName']
        elif 'code' in section_code_element.attrib:
            section_type = section_code_element.attrib['code']

    # just a find doesn't work
    #section_code_element = section_element.find(OBSERVATION_SECTION_CODE, ns)

    for observation in section_element.findall(OBSERVATION_PATH, ns):
        print(f"{section_type}, ", end='')
        for code_element in observation.findall('./code', ns):
            vocabulary_id = oid_map[code_element.attrib['codeSystem']][0]
            print(f"{code_element.attrib['displayName']}, {vocabulary_id}, {code_element.attrib['code']},", end=' ')

        display_string=""
        for value_element in observation.findall('./value', ns):
            if 'value' in value_element.attrib: 
                display_string = f"{display_string}, {value_element.attrib['value']} "
            else:
                display_string = f"{display_string}, None "
            if 'unit' in value_element.attrib: 
                display_string = f"{display_string}, {value_element.attrib['unit']}  "
            else:
                display_string = f"{display_string}, None "
            if 'xsi:type' in value_element.attrib: 
                display_string = f"{display_string}, {value_element.attrib['xsi:type']} "
            else:
                display_string = f"{display_string}, None "
        print(display_string, end='')
        print("")
    
        





uselese = """
        for code_element in tree.findall(path, ns):
            try:
                vocabulary_oid = code_element.attrib['codeSystem']
                vocabulary_id = oid_map[vocabulary_oid][0]
                concept_code = code_element.attrib['code']
                details = VocabSpark.lookup_omop_details(spark, vocabulary_id, concept_code)
                if details is not None:
                    concept_name = details[2]
                    domain_id = details[3]
                    class_id = details[4]
                    print((f"{path}  vocab:{vocabulary_id} code:{concept_code}"
                           " \"{concept_name}\" domain:{domain_id} class:{class_id}"))
                else:
                    print((f"{path}  vocab:{vocabulary_id} code:{concept_code} "
                          "(code not available in OMOP vocabulary here)"))
            except Exception:
                print((f"{path}  -- no attributes, or not both --"
                      f" oid:{vocabulary_oid}  code:{concept_code}"))
"""
