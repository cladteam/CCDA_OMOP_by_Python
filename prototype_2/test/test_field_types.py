import unittest
import io
from lxml import etree as ET
from collections import defaultdict
import prototype_2.value_transformations as VT
from prototype_2.data_driven_parse import parse_config_from_xml_file

class FieldTypeTest_DERIVED(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xml_text = """
        <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:voc="urn:hl7-org:v3/voc" xmlns:sdtc="urn:hl7-org:sdtc">
            <recordTarget>
                <patientRole>
                    <id extension="444222222" root="2.16.840.1.113883.4.1"/>
                    <code code="742-7" codeSystem="2.16.840.1.113883.6.1"/>
                    <addr use="HP">
                        <streetAddressLine>2222 Home Street</streetAddressLine>
                        <city>Beaverton</city>
                        <state>MD</state>
                        <postalCode>21014</postalCode>
                        <country>US</country>
                    </addr>
                </patientRole>
            </recordTarget>
        </ClinicalDocument>
        """
        self.config_dict = {

            'Test': {
                'root': {
                    'config_type': 'ROOT',
                    'expected_domain_id' : 'Test',
                    'element': "./hl7:recordTarget/hl7:patientRole"
                },
                'concept_codeSystem': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "codeSystem",
                    'order': 3
                },
                'concept_code': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "code",
                    'order': 4
                },
               	'concept_id': {
    	            'config_type': 'DERIVED',
    	            'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	            'argument_names': {
    		            'concept_code': 'concept_code',
    		            'vocabulary_oid': 'concept_codeSystem',
                        'default': 0
    	            },
                    'order': 8
    	        },
              	'domain_id': {
    	            'config_type': 'DERIVED',
    	            'FUNCTION': VT.map_hl7_to_omop_domain_id,
    	            'argument_names': {
    		            'concept_code': 'concept_code',
    		            'vocabulary_oid': 'concept_codeSystem',
                        'default': 0
    	            },
                    'order': 9
    	        }
            }
        }

    def test(self):
            # a deep test that not only tests the DERVIED mechanisms, but the availability
            # of the map file.
            # 2.16.840.1.113883.6.1,742-7,3033575,Measurement
        with io.StringIO(self.xml_text) as file_obj:
            tree = ET.parse(file_obj)
            pk_dict = {}
            for domain, domain_meta_dict in self.config_dict.items():
                # print(f"INPUT {domain} {domain_meta_dict}")
                data_dict_list= parse_config_from_xml_file(tree, domain, domain_meta_dict, "test_file", pk_dict)
                data_dict = data_dict_list[0]
                #print(f"OUTPUT {data_dict}")
                self.assertEqual(data_dict['concept_codeSystem'], "2.16.840.1.113883.6.1")
                self.assertEqual(data_dict['concept_code'], "742-7")
                self.assertEqual(data_dict['concept_id'], 3033575)
                self.assertEqual(data_dict['domain_id'], "Measurement")
            
            
            
class FieldTypeTest_FIELD(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xml_text = """
        <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:voc="urn:hl7-org:v3/voc" xmlns:sdtc="urn:hl7-org:sdtc">
            <recordTarget>
                <patientRole>
                    <id extension="444222222" root="2.16.840.1.113883.4.1"/>
                    <addr use="HP">
                        <streetAddressLine>2222 Home Street</streetAddressLine>
                        <city>Beaverton</city>
                        <state>MD</state>
                        <postalCode>21014</postalCode>
                        <country>US</country>
                    </addr>
                </patientRole>
            </recordTarget>
        </ClinicalDocument>
        """
        self.config_dict = {

            'Test': {
                'root': {
                    'config_type': 'ROOT',
                    'expected_domain_id' : 'Test',
                    'element': "./hl7:recordTarget/hl7:patientRole"
                },
                'attribute_value': {
                    'config_type': 'FIELD',
                    'element': 'hl7:id',
                    'attribute': "root",
                    'order': 1
                },
                'text_value': {
                    'config_type': 'FIELD',
                    'element': 'hl7:addr/hl7:streetAddressLine',
                    'attribute': "#text",
                    'order': 2
                }
            }
        }

    def test(self):
        with io.StringIO(self.xml_text) as file_obj:
            tree = ET.parse(file_obj)
            pk_dict = {}
            for domain, domain_meta_dict in self.config_dict.items():
                #print(f"INPUT {domain} {domain_meta_dict}")
                data_dict_list= parse_config_from_xml_file(tree, domain, domain_meta_dict, "test_file", pk_dict)
                data_dict = data_dict_list[0]
                #print(f"OUTPUT {data_dict}")
                self.assertEqual(data_dict['attribute_value'], "2.16.840.1.113883.4.1")
                self.assertEqual(data_dict['text_value'], "2222 Home Street")

                
class FieldTypeTest_HASH(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xml_text = """
        <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:voc="urn:hl7-org:v3/voc" xmlns:sdtc="urn:hl7-org:sdtc">
            <recordTarget>
                <patientRole>
                    <id extension="444222222" root="2.16.840.1.113883.4.1"/>
                    <code code="742-7" codeSystem="2.16.840.1.113883.6.1"/>
                    <addr use="HP">
                        <streetAddressLine>2222 Home Street</streetAddressLine>
                        <city>Beaverton</city>
                        <state>MD</state>
                        <postalCode>21014</postalCode>
                        <country>US</country>
                    </addr>
                </patientRole>
            </recordTarget>
        </ClinicalDocument>
        """
        self.config_dict = {
            'Test': {
                'root': {
                    'config_type': 'ROOT',
                    'expected_domain_id' : 'Test',
                    'element': "./hl7:recordTarget/hl7:patientRole"
                },
                'concept_codeSystem': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "codeSystem"
                },
                'concept_code': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "code"
                },
                'test_hash_0': { 
                    'config_type': 'HASH',
                    'fields' : [ 'concept_codeSystem', 'concept_code' ], 
                    'order' : 0
                },
               	'test_hash_1': { 
                    'config_type': 'HASH',
                    'fields' : [ 'concept_code', 'concept_codeSystem' ], 
                    'order' : 1
                },
                'test_hash_2': { 
                    'config_type': 'HASH',
                    'fields' : [ 'concept_code', None ], 
                    'order' : 2
                },
                'test_hash_3': { 
                    'config_type': 'HASH',
                    'fields' : [ None, 'concept_codeSystem' ], 
                    'order' : 3
                },
                'test_hash_4': { 
                    'config_type': 'HASH',
                    'fields' : [ None ], 
                    'order' : 4
                }
            }
        }

    def test(self):
        with io.StringIO(self.xml_text) as file_obj:
            tree = ET.parse(file_obj)
            pk_dict = defaultdict(list)
            for domain, domain_meta_dict in self.config_dict.items():
                data_dict_list= parse_config_from_xml_file(tree, domain, domain_meta_dict, "test_file", pk_dict)
                data_dict = data_dict_list[0]
                print(f"OUTPUT {data_dict}")
                #self.assertEqual(data_dict['test_hash_0'][0], 2730455650958355)
                #self.assertEqual(data_dict['test_hash_1'][0], 1347390606787380)
                #self.assertEqual(data_dict['test_hash_2'][0], 1136581816084342)
                #self.assertEqual(data_dict['test_hash_3'][0], 2914246837974734)
                #self.assertEqual(data_dict['test_hash_4'][0], None)
                self.assertEqual(data_dict['test_hash_0'], 2730455650958355)
                self.assertEqual(data_dict['test_hash_1'], 1347390606787380)
                self.assertEqual(data_dict['test_hash_2'], 1136581816084342)
                self.assertEqual(data_dict['test_hash_3'], 2914246837974734)
                self.assertEqual(data_dict['test_hash_4'], None)

            
            

