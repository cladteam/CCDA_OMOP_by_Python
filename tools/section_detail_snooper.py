#!/usr/bin/env python3

"""
    section_detail_snooper - looks for specfic sections driven by metadata,
        and shows any ID, CODE and VALUE elements within them.
	outputting these details in a file per section, per file.

    TODO: the list after a path in metadata isn't used currently.
        Ultimately it would replce the hard-coded id, code, value
        and effectiveTime parts

    TODO: templateIds vs concepts?
"""

import argparse
import os
import xml.etree.ElementTree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
from vocab_map_file import oid_map
from xml_ns import ns

subs_admin_prefix = './entry/act/entryRelationship/substanceAdministration'


section_metadata = {

# Sections ...10.20.22.2.[ 1.1, 3.1, 4.1, 7.1, 14, 22, 22.1, 41 ]

    'Encounters' : {
    	'loinc_code' : '46240-8',
        'root' : ["2.16.840.1.113883.10.20.22.2.22.1", "2.16.840.1.113883.10.20.22.2.22"],
        'element' : "./entry/encounter",
	'sub_elements' :  ['encounter']
    },

    'Medications': { 
        'loinc_code':'10160-0',
        'root' : [ "2.16.840.1.113883.10.20.22.2.1.1",  "2.16.840.1.113883.10.20.22.2.1"],
        'element' : "./entry/substanceAdministration/",
        'sub_elements' : [
            "consumable/manufacturedProduct/manufacturedMaterial",
            "performer",
            "entryRelationship/observation",
            "entryRelationship/supply/product/manufacturedProduct/manufacturedMaterial"]
    },

    'Hospital Discharge': {
        'loinc_code':'10183-2',
        'root' : ["2.16.840.1.113883.10.20.22.2.41"],
        'element' : "./entry/act/", 
        'sub_elements' : ['id', 'code', 'effectiveTime',
                          'entryRelationship/substanceAdministration']
        #(subs_admin_prefix + "/consumable/manufacturedProduct/manufacturedMaterial"): [],
        #(subs_admin_prefix + "/performer/assignedEntity"): [],
        #(subs_admin_prefix + "/performer/assignedEntity/representedOrganization"): []
    },

    'Procedures': { 
        'loinc_code' : '47519-4',
	'root' : ["2.16.840.1.113883.10.20.22.2.7.1"],
        'element' : "./entry/procedure",
        'sub_elements' : ['procedure']
    },

    'Results': {
        'loinc_code' : '30954-2',
        'root' : ["2.16.840.1.113883.10.20.22.2.3.1"],
        'element' : "./entry/organizer/component/observation",
        'sub_elements' :  ['observation']
    },
    # (ToDo these observatoins appear in multiple places?

    'Vital Signs':  {
        'loinc_code' : '8716-3', 
        'root' : [ "2.16.840.1.113883.10.20.22.2.4.1", "2.16.840.1.113883.10.20.22.2.4" ],
        'element' : "./entry/organizer/component/observation",
        'sub_elements' : ['observation']  
    },

# Sections Out of Scope 2.16.840.1.113883.10.20.22.2.[2, 2.1, 5.1, 6.1, 8, 9, 10, 11, 12 ,14, 15, 17, 18, 21, 45]
# Sections Out of Scope 2.16.840.1.113883.10.20.22.1.[6]
# Sections Out of Scope 1.3.6.1.4.1.19376.1.5.3.1.3.1
# Section Immunizations OoS   'root'="2.16.840.1.113883.10.20.1.6"/>
# Section Immunizations OoS   'root'="2.16.840.1.113883.10.20.22.2.2.1"/>
# Section Immunizations OoS   'root'="2.16.840.1.113883.10.20.22.2.2"/>
#    '11369-6': { "./entry/substanceAdministration/consumable/manufacturedProduct/manufacturedMaterial"

# Section Problem List OoS?    root="2.16.840.1.113883.10.20.22.2.5.1"/>
# Section Allergies OoS       'root'="2.16.840.1.113883.10.20.22.2.6.1"/>
#    '48765-2': { './entry/': [], './entry/act/effectiveTime': [],
#        './entry/act/entryRelationship/observation': [],
#        './entry/act/entryRelationship/observation/entryRelationship/observation': [],

# Section Functional and Cognitive Status  'root'="2.16.840.1.113883.10.20.22.2.14"/>
#    '474020-5': {  # FUNCTIONAL STATUS (observations)
#        "./entry/observation":  ['observation']

# Section Assessments  OoS             root="2.16.840.1.113883.10.20.22.2.8"/>
# Section ????  OoS                    root="2.16.840.1.113883.10.20.22.2.9"/>
# Section Care Plan OoS                root="2.16.840.1.113883.10.20.22.2.10"/>
# Section Discharge Medications OoS?   root="2.16.840.1.113883.10.20.22.2.11.1"/>
# Section Reason For Visit OoS?        root="2.16.840.1.113883.10.20.22.2.12"/>
# Section Family History OoS           root="2.16.840.1.113883.10.20.22.2.15"/>
# Section Social History OsS           root="2.16.840.1.113883.10.20.22.2.17"/>
# Section Payers OsS                   root="2.16.840.1.113883.10.20.22.2.18"/>
# Section Reason for Referral OoS      root="1.3.6.1.4.1.19376.1.5.3.1.3.1"/>
# Section Advanced Directives OoS      root="2.16.840.1.113883.10.20.22.2.21"/>
# Section Instructions OoS      root="2.16.840.1.113883.10.20.22.2.45"/>

}


def scan_section(base_name, section_name, section_element):
    output_filename = f"{base_name}_{section_name}.log"
    print(f"output: {output_filename}")
    with  open(output_filename, 'w', encoding="utf-8") as f:
        i=0
        for section_element in section_elements:
            i += 1
            print(f"{output_filename}  {i} ")
            for section_code_element in section_element.findall('code', ns):
                display_name=""
                code=""
                codeSystem=""
                codeType=""
                if 'displayName' in section_code_element.attrib:
                    display_name = section_code_element.attrib['displayName']
                if 'code' in section_code_element.attrib:
                    code = section_code_element.attrib['code']
                if 'codeSystem' in section_code_element.attrib:
                    code_system = section_code_element.attrib['codeSystem']
                #if codeType == '':
                #    if section_code_system in oid_map:
                #        vocab = oid_map[section_code_system][0]
                #        #section_type = details[2]
                #        #details = VocabSpark.lookup_omop_details(spark, vocab, section_code)
                #section_type=""
                #section_code=""
                #section_codeSystem=""
                #section_codeType=""
                f.write(f" {i} {codeSystem} {code} {displayName}")

def scan_file(filename):
    base_name = os.path.basename(filename)

    tree = ET.parse(filename)
    for section_name, section_details in section_metadata.items():
        print(f"{base_name} {section_name}")
        for template_id in section_details['root']:
            section_path = f"./component/structuredBody/component/section/templateId[@root=\"{template_id}\"]/.."
            print(f"section path {section_path}")
            section_element_list = tree.findall(section_path, ns)
            print(F" length of element list: {len(section_element_list)}")
            for section_element in section_element_list:
                print(f"    {base_name} {section_name}")
                scan_section(section_name, section_element, section_name)
    
    
#        print(f"SECTION type:\"{section_type}\" code:\"{section_code}\" ", end='')
#        section_code = section_code_element.attrib['code']
#        if section_code is not None and section_code in section_metadata:
#            print("")
#            for entity_path in section_metadata[section_code]:
#                print(f"  MD section code: \"{section_code}\" path: \"{entity_path}\" ")
#                for entity in section_element.findall(entity_path, ns):
#                    print((f"    type:\"{section_type}\" code:\"{section_code}\", "
#                           f" tag:{entity.tag} attrib:{entity.attrib}"), end='')
#    
#                    # show_effective_time(entity)
#    
#                    # referenceRange
#    
#                    # show_id(entity)
#    
#                    # show_code(entity)
#    
#                    # show_value(entity)
#                    print("")
#        else:
#            print(f"No metadata for \"{section_code}\"     ")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    parser.add_argument('-f', '--filename', help="filename to parse")
    args = parser.parse_args()

    scan_file(args.filename)
    #scan_file('resources/CCDA_CCD_b1_Ambulatory_v2.xml')
    # scan_file('resources/CCDA_CCD_b1_InPatient_v2.xml')
