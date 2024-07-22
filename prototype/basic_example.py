#!/usr/bin/env python3

"""
    A basic example for parsing an XML file, fetching some attribute values,
    and converting them to an OMOP concept_id.
    Uses some support for keeping the OMOP concept table in Spark.
"""

import xml.etree.ElementTree as ET
from util import vocab_spark
from util.xml_ns import ns
from util import spark_util

INPUT_FILENAME = 'resources/CCDA_CCD_b1_InPatient_v2.xml'

# INIT SPARK
spark_util_object = spark_util.SparkUtil()

# PARSE/LOAD
tree = ET.parse(INPUT_FILENAME)

# FIND JUST ONE: using find
#observation = tree.find(
#    "./component/structuredBody/component/section/entry/organizer/component/observation", ns)
#observation_code = observation.find("code", ns)
#print(f"single:  {observation_code.get('code')}")



# USE A PATH
print("=================")

observations = tree.findall(".//section/templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']/../entry/organizer/component/observation", ns)
for obs in observations:
    print(f"{obs.tag}========")
    code = obs.find("./code", ns)
    if code is not None:
        print(f"        {code.attrib['displayName']}")
exit()



id2_element_list = tree.findall("./recordTarget/patientRole/id", ns)
print(id2_element_list)
for ide in id2_element_list:
    print(f" {ide.tag}  {ide.attrib}")

print("=================")
id2_element = tree.find("./recordTarget/patientRole/id[@root='2.16.840.1.113883.4.1']",ns)
print(id2_element)
id2 = id2_element.get('extension')
print(id2)
print("=================")
exit()



# FIND DATA: using findall
observations = tree.findall(
    "./component/structuredBody/component/section/entry/organizer/component/observation", ns)

for observation in observations:
    observation_code = observation.find("code", ns)
    vocabulary_id = observation_code.attrib['codeSystem']
    concept_code = observation_code.attrib['code']

    # CONVERT TO OMOP
    observation_concept_id = vocab_spark.map_hl7_to_omop(
        vocabulary_id,
        concept_code)

    # OUTPUT
    print((f"vocaublary_id {vocabulary_id} concept_code {concept_code} "
           f"maps to {observation_concept_id}"))
resources/170.314b2_AmbulatoryToC.xml
resources/CCDA_CCD_b1_Ambulatory_v2.xml
resources/CCDA_CCD_b1_InPatient_v2.xml
resources/Inpatient_Encounter_Discharged_to_Rehab_Location(C-CDA2.1).xml
resources/ToC_CCDA_CCD_CompGuideSample_FullXML.xml
resources/test_2.out.xml
resources/test_2.xml
