#!/usr/bin/env python3
"""
    header_code_snooper -  finds and outputs code elements found under certain header elements

"""
import argparse
import xml.etree.ElementTree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from xml_ns import ns
from vocab_map_file import oid_map


header_elements = [
    'recordTarget/patientRole/patient',
    'documentationOf/serviceEvent',
    'documentationOf/serviceEvent/performer/assignedEntity',
    'componentOf/encompassingEncounter'
]


def dump_file(filename):
    tree = ET.parse(args.filename)
    for element_path in header_elements:
        for element in tree.findall(f"{element_path}//code", ns):
            print(f"{element_path}  {element.attrib}")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    parser.add_argument('-f', '--filename', help="filename to parse")
    args = parser.parse_args()

    dump_file(args.filename)
