#!/usr/bin/env python3
"""
    header_snooper - driven by three levels of metadata for top-level header elements,
    middle elements, and attributes, shows what is foudn in the header. Mostly
    involving time, assinged person, assigned entity and encompassing encounter.

header_elements --> middle_elements --> element_attributes
and sometimes middle_elements --> middle_elements

INNOVATION: This snooper takes on types and paths more clearly than past hacks.
            Using paths makes it easier to skip levels. Using types (symbols in
            capital letters that don't appear in the path, and only sometimes in
            the documenation as initial capital camel case) keeps path elements and
            types separate, making it easier to jump from one level to the next.
TODO:
    - some vocabularies are not in the oid_map in util/vocab_map_file
    - need to put encounters, patients and providers into a map in order to link them
      with things coming out of the sections
    - need to better identify the mapping to OMOP
"""
import re
import argparse
import xml.etree.ElementTree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from xml_ns import ns
from vocab_map_file import oid_map


# everything is optional, entities may repeat

element_attributes = {  # (or sub-elements with text)
    'ID': ['root', 'extension'],
    'CODE': ['code', 'codeSystem', 'displayName'],

    'NAME': ['given', 'family', 'prefix'],
    'TIME': ['value'],  # <time value=x>
    'ADDR': ['streetAddressLine', 'city', 'state', 'postalCode', 'country'],

    'TEXT': ["a special case meaning it's a pair of tags surroudning text"],
    'LOW_TIME': ['value'],  # <time value=x>
    'HIGH_TIME': ['value'],  # <time value=x>
}

middle_elements = {
    'CODE': 
        {'skip' : None },
    'TIME':  {
        'low': 'LOW_TIME', 
        'high': 'HIGH_TIME'},
    'ASSIGNED_PERSON': {
        'name': 'NAME'},
    'REPRESENTED_ORGANIZATION': {
        'id': 'ID', 
        'name': 'TEXT', 
        'addr': 'ADDR'},
    'ASSIGNED_ENTITY': {
        'id': 'ID', 
        'code': 'CODE', 
        'addr': 'ADDR',
        'assignedPerson': 'ASSIGNED_PERSON',
        'representedOrganization': 'REPRESENTED_ORGANIZATION'},
    'ENCOMPASSING_ENCOUNTER': {
        'id': 'ID', 
        'code': 'CODE', 
        'effectiveTime': 'TIME',
        'responsbleParty/assignedEntity': 'ASSIGNED_ENTITY',
        'encounterParticipant/assignedEntity': 'ASSIGNED_ENTITY',
        'location/healthcareFacility': 'ID'}
}

header_elements = {
    # PATIENT
    'recordTarget/patientRole/id': 'ID',
    'recordTarget/patientRole/patient/name': 'NAME',
    'recordTarget/patientRole/patient/administrativeGenderCode': 'CODE',
    'recordTarget/patientRole/patient/raceCode': 'CODE',
    'recordTarget/patientRole/patient/ethnicGroupCode': 'CODE',

    #  VISIT type from here?
    'documentationOf/serviceEvent/code': 'CODE',
    'documentationOf/serviceEvent/effectiveTime': 'TIME',

    # PROVIDER and/or CARE_SITE
    'documentationOf/serviceEvent/performer/functionCode': 'CODE',
    'documentationOf/serviceEvent/performer/time': 'TIME',
    'documentationOf/serviceEvent/performer/assignedEntity': 'ASSIGNED_ENTITY',

    # VISIT provider and dates, care_site
    'componentOf/encompassingEncounter': 'ENCOMPASSING_ENCOUNTER'
}


def dump_attributes(element, element_type):
    if type != 'TEXT':
        if element_type in element_attributes:
            for attr in element_attributes[element_type]:
                if attr in element.attrib:
                    if attr == 'root' or attr == 'codeSystem' and element.attrib[attr] in oid_map:
                        print((f"        A {re.sub(r'{.*}', '', element.tag)}.{attr}: "
                               f"{oid_map[element.attrib[attr]][0]} "
                               f"{oid_map[element.attrib[attr]][1]} "))
                    else:
                        print((f"        B {re.sub(r'{.*}', '', element.tag)}.{attr}:"
                               f" {element.attrib[attr]} "))
                else:
                    attr_ele = element.find(attr, ns)
                    if attr_ele is not None:
                        print((f"        C {re.sub(r'{.*}', '', element.tag)}.{attr}:"
                               f" {attr_ele.text}"))
                    else:
                        print(f"        D None")
    else:
        print(f"        E {re.sub(r'{.*}', '', element.tag)}.{element.text}: ")

def dump_middle(middle_element, middle_type):
    if middle_type in middle_elements:
        for (ele_path, ele_type) in middle_elements[middle_type].items():
            print(f"    {ele_path} {ele_type}     {middle_type}")
            if ele_path == 'skip':
                    dump_attributes(middle_element, middle_type)
            else:
                elements = middle_element.findall(f"./{ele_path}", ns)
                for ele in elements:
                    dump_attributes(ele, ele_type)

def dump_file(filename):
    tree = ET.parse(args.filename)
    for (element_path, element_type) in header_elements.items():
        for element in tree.findall(element_path, ns):
            print(f"{element_path} {element_type} {re.sub(r'{.*}', '', element.tag)}")
            if element_type in middle_elements:
                dump_middle(element, element_type)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    parser.add_argument('-f', '--filename', help="filename to parse")
    args = parser.parse_args()

    dump_file(args.filename)
