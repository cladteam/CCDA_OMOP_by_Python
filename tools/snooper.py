
"""
    snooper - Like Snooper, looks for elements with the given name, fetches what
              attributes it can, mapping OIDs to vocabularies and
              concept codes to names when it can,
              lists the paths to the elements with their attributes.
              This means there are special cases for elements tagged 'code',
              and (soon) 'section'.
              This version, filters on section types.
    Notes:
        patientRole
            recordTarget/patientRole
            recordTarget/patientRole/patient
        assignedEntity
            performer/assignedEntity
            responsibleParty/assignedEntity
            encounterParticipant/assignedEntity
            assignedEntity/assignedPerson ...but not always

    Examples:
      python -m   tools.snooper --tag=code -v=True   
        Finds all paths to "code" elements and shows their attributes.

      python -m   tools.snooper --tag=section --shallow=True -v=True
        Finds all paths to "sections" elements and shows their attributes

      python -m   tools.snooper --tag=section  -v=True
        Finds all paths to "sections" elements, 
        checking/filtering that a code element immediately below has the specified codesystema and code, 
        then shows specific detail specifed in metadata below
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

parser.add_argument('-v', '--verbose', default=False,
                    help="verbose output")
parser.add_argument('-sh', '--shallow', default=False,
                    help="just shows the tags code parts, doesn't filter and dig deeper")
# Sections:
# section code: 11369-6 is immunizations
# section code: 46240-8 is encounters
# section code: 10160-0 is medications
# section code: 47519-4 is procedures
# section code; 30954-2 is results
# section code: 8716-3  is vital signs
parser.add_argument('-c', '--code', default='30954-2',
                    help="code attribute of a code element under the element with the tag in the -t option")

parser.add_argument('-cs', '--codeSystem', default='2.16.840.1.113883.6.1',
                    help=("codeSystem attribute of a code element under the element with the tag"
                          " in the -t option, specified as a HL7 OID"))
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


def element_has_specific_code(element, codeSystem, code):
    """ Checks to see if the passed-in element has a sub element named 
        code with attributes codeSystem and and code that match
        what was passed in.
    """
    trimmed_tag = re.sub(r"{.*}", '', element.tag)
    if trimmed_tag == 'code':
        code_element = element
    else:
        code_element = element.find('code', ns)
    if code_element is None:
        print(f"element_has_specific_code FALSE {element.tag}")
        return False

    if 'code' in code_element.attrib and 'codeSystem' in code_element.attrib:
        if code_element.attrib['code'] == code  and \
               code_element.attrib['codeSystem'] == codeSystem :
            if args.verbose:
                print(f"element_has_specific_code and both match {code_element.attrib} {code} {codeSystem}")
            return True
        elif args.verbose:
            print(f"element_has_specific_code both do not matchFALSE {code_element.attrib} {code} {codeSystem}")
    elif args.verbose:
        print(f"element_has_specific_code does not have code and codeSystem attributes {code_element.attrib}")

    return False

def element_has_code(element):
    """ Checks to see if the passed-in element has a sub element named code.
    """

    trimmed_tag = re.sub(r"{.*}", '', element.tag)
    if trimmed_tag == 'code':
        code_element = element
    else:
        code_element = element.find('code', ns)

    if code_element is not None:
        if 'code' in code_element.attrib and 'codeSystem' in code_element.attrib:
            return code_element
        if args.verbose:
            trimmed_code_tag = re.sub(r"{.*}", '', code_element.tag)
            print(f"element_has_code() did not find code/codeSystem attributes in this guy {trimmed_code_tag} {code_element.attrib}")
            print(f"     was looking below {trimmed_tag} {element.attrib}")

    if args.verbose:
        print(f"element_has_code() did not find a code below this guy {element.tag} {element.attrib}")

    return None

# LOINC code maps to a list of tuples. Each tuple has a name and path.
# MED PROC?   section/entry/substanceAdministration/entryRelationship/act/code
section_metadata = {
    '11369-6' : [('immunizations', 
                  "section/entry/substanceAdministration/consumable/manufacturedProduct/manufacturedMaterial/code")],
    '46240-8' : [('encounters', 
                  "section/entry/encounter/code")],
    '10160-0' : [
        ('medications', 
         "section/entry/substanceAdministration/consumable/manufacturedProduct/manufacturedMaterial/code"),
        ('medications', 
         "section/entry/substanceAdministration/entryRelationship/supply/product/manufacturedProduct/manufacturedMaterial/code") 
    ],
    '47519-4' : [('procedures', "section/entry/procedure/code")],
    '30954-2' : [('results', "section/entry/organizer/component/observation/code")],
    '8716-3' : [('vital signs', "section/entry/organizer/component/observation/code")]
}

def dump_element(section_element, code):
    """  Prints more detail about a section
         Input: the section of an element
         Returns: attributes about the concept, see parse_code_element_to_omop() above.
    
         Follows one of a few possible paths down from a section.
         The idea here is to explore domains and domain_id mapping.
         Not to go deeper with values or dates.
    """

    for path_tuple in section_metadata[code]:
        section_name = path_tuple[0] 
        path = path_tuple[1] 
        for code_element in element.find(path):
           parts = parse_code_element_to_omop(code_element)
           print((f"    {trimmed_tag}  {parts[1]} {parts[2]}"
                   " {parts[3]} \"{parts[5]}\" {parts[6]} "))

def snoop_tag(tag, codeSystem, code):
    ''' Looks for entities at the bottom (leaves) of the tree named with the given tag.
        Prints out all sorts of detail including attributes.
        If the entity is a code entity, this function digs deeper and translates
        relevant attributes to OMOP.
        if the entity is some other entity it looks for a code and only
        continues with those whose codeSystem/code match
    '''
    for path in path_gen(INPUT_FILENAME):
        if re.fullmatch(f".*/{tag}", path):
            i = 0
            for element in tree.findall(path, ns):
                print(f"{i} {tag} {code} {path} ")
                trimmed_tag = re.sub(r"{.*}", '', element.tag)
                if trimmed_tag == 'code':
                    parts = parse_code_element_to_omop(element)
                    print((f"    (code):    {trimmed_tag}  {parts[1]} {parts[2]}"
                          f" {parts[3]} \"{parts[5]}\" {parts[6]} "))
                else:
                    if args.shallow:
                        code_element = element_has_code(element)
                        if code_element is not None:
                            parts = parse_code_element_to_omop(code_element)
                            print((f"   {tag}    {trimmed_tag}  {parts[1]} {parts[2]}"
                                   f" {parts[3]} \"{parts[5]}\" {parts[6]} "))
                            print("\n")
                        elif args.verbose:
                            print(f"shallow, no code {element.tag} {element.attrib}")
                
                    else:
                        if element_has_specific_code(element, codeSystem, code):
                            try:
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
                        elif args.verbose:
                            trimmed_tag = re.sub(r"{.*}", '', element.tag)
                            print(f"     no specific code element? {trimmed_tag} {path} {codeSystem} {code}")
            i += 1


# -- Main --
tree = ET.parse(args.filename)
snoop_tag(args.tag, args.codeSystem, args.code)

# snoop_tag("code") # code, CodeSystem, codeSystemName, displayName

# TODO: what are the root OIDs equivalent to?
# snoop_tag("id") # root and translation

# snoop_tag("assignedEntity") # no attributes?
# snoop_tag("assignedPerson") # no attributes?

# Noteable
# snoop_tag("patientRole") # 1 row, no attributesd
# snoop_tag("patient") #1 row, no attributes, but interesting sub elements
