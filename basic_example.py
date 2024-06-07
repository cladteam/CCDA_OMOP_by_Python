#!/usr/bin/env python3

import xml.etree.ElementTree as ET
from util import vocab_spark
from util.xml_ns import ns
from util import spark_util

input_filename = 'resources/CCDA_CCD_b1_InPatient_v2.xml'

# INIT SPARK for the 
spark_util_object = spark_util.SparkUtil()

# PARSE/LOAD
tree = ET.parse(input_filename)

# FIND DATA
observations = tree.findall("./component/structuredBody/component/section/entry/organizer/component/observation", ns)

for observation in observations:
    observation_code = observation.find("code", ns)
    vocabulary_id = observation_code.attrib['codeSystem']
    concept_code = observation_code.attrib['code']

    # CONVERT TO OMOP
    observation_concept_id = vocab_spark.map_hl7_to_omop(vocabulary_id, 
        concept_code)

    # OUTPUT
    print(f"vocaublary_id {vocabulary_id} concept_code {concept_code} maps to {observation_concept_id}")
