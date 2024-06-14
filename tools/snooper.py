
"""
    Snooper - looks for elements with the given name, fetches what
              attributes it can, mapping OIDs to vocabularies and
              concept codes to names when it can,
              lists the paths to the elements with their attributes.
              This means there are special cases for elements tagged 'code',
              and (soon) 'section'.
    Notes:
        patientRole
            recordTarget/patientRole
            recordTarget/patientRole/patient
        assignedEntity
            performer/assignedEntity
            responsibleParty/assignedEntity
            encounterParticipant/assignedEntity
            assignedEntity/assignedPerson ...but not always
"""

import argparse
import xml.etree.ElementTree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
import re  # https://docs.python.org/3.9/library/re.html
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
parser.add_argument('-t', '--tag', default='code',
                    help="tag name to filter by. Try patient, assignedPerson or code")
args = parser.parse_args()


def path_gen(filename):
    '''
        Generates tag paths to leaves in the XML tree from the file
        whose name is passed in.

        credit: https://stackoverflow.com/questions/68215347/
         capture-all-xml-element-paths-using-xml-etree-elementtree
    '''
    path = []
    it = ET.iterparse(filename, events=('start', 'end'))
    for evt, el in it:
        if evt == 'start':
            # trim off namespace stuff in curly braces
            trimmed_tag = re.sub(r"{.*}", '', el.tag)

            # don't include the "/ClinicalDocument" part at the very start
            if trimmed_tag == 'ClinicalDocument':
                path.append(".")
            else:
                path.append(trimmed_tag)

            yield '/'.join(path)
        else:
            path.pop()


def parse_code_element_to_omop(element):
    ''' given a code element from a CCDA document, this function converts
        its codeSystem OID to a vocabulary id, then uses it and the code
        to get detail from the OMOP concept table.
    '''

    code_system_name = 'n/a'
    display_name = 'n/a'
    if 'codeSystem' in element.attrib:
        vocabulary_oid = element.attrib['codeSystem']
        vocabulary_id = oid_map[vocabulary_oid][0]
        concept_code = element.attrib['code']

        # codeSystemName should echo vocabulary_id
        if 'codeSystemName' in element.attrib:
            code_system_name = element.attrib['codeSystemName']
            if code_system_name != vocabulary_id:
                print((f"INFO **** != {vocabulary_id} != {code_system_name}"
                      " *** different vocabulary_id & code_system names"))

        # displayName doesn't seem to always echo concept_name
        if 'displayName' in element.attrib:
            display_name = element.attrib['displayName']

        details = VocabSpark.lookup_omop_details(spark, vocabulary_id, concept_code)
        concept_id = "n/a"
        concept_name = "n/a"
        domain_id = "n/a"
        class_id = "n/a"
        if details is not None:
            concept_id = details[1]
            concept_name = details[2]
            domain_id = details[3]
            class_id = details[4]

    # TODO
    # if this element is a section, give some detail
    # if this element is a person or patient, give some detail about elements below it.
        return (vocabulary_oid, vocabulary_id, concept_code, domain_id, class_id,
                concept_name, concept_id, code_system_name, display_name)

    elif 'code' in element.attrib:
        return ('n/a', 'n/a', element.attrib['code'], 'n/a',
                'n/a', 'n/a', 'n/a', code_system_name, display_name)
    else:
        return ('n/a', 'n/a', 'n/a', 'n/a',
                'n/a', 'n/a', 'n/a', code_system_name, display_name)


def snoop_tag(tag):
    ''' Looks for entities at the bottom (leaves) of the tree named with the given tag.
        Prints out all sorts of detail including attributes.
        If the entity is a code entity, this function digs deeper and translates
        relevant attributes to OMOP.
    '''
    for path in path_gen(INPUT_FILENAME):
        if re.fullmatch(f".*/{tag}", path):
            i = 0
            for element in tree.findall(path, ns):
                trimmed_tag = re.sub(r"{.*}", '', element.tag)
                print(f"{i} {path}")
                if trimmed_tag == 'code':
                    parts = parse_code_element_to_omop(element)
                    print((f"    {trimmed_tag}  {parts[1]} {parts[2]}"
                          " {parts[3]} \"{parts[5]}\" {parts[6]} "))
                else:
                    try:
                        print(f"{i} {path}")
                        for attribute_name in element.attrib:
                            attribute_value = 'n/a'
                            try:
                                attribute_value = element.get(attribute_name)
                            except Exception:
                                attribute_value = 'N/A'
                            print(f"    {trimmed_tag}  {attribute_name} {attribute_value}")
                    except RuntimeError as error:
                        print(error)
                        print(f"{i} {path}  -- no attributes, or not both -- ")
            i += 1


# -- Main --
tree = ET.parse(args.filename)
snoop_tag(args.tag)

# snoop_tag("code") # code, CodeSystem, codeSystemName, displayName

# TODO: what are the root OIDs equivalent to?
# snoop_tag("id") # root and translation

# snoop_tag("assignedEntity") # no attributes?
# snoop_tag("assignedPerson") # no attributes?

# Noteable
# snoop_tag("patientRole") # 1 row, no attributesd
# snoop_tag("patient") #1 row, no attributes, but interesting sub elements
