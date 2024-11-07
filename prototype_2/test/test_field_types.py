import unittest
import io
import prototype_2.value_transformations as VT
from lxml import etree as ET
from prototype_2.data_driven_parse import parse_domain_from_dict

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
                data_dict_list= parse_domain_from_dict(tree, domain, domain_meta_dict, "test_file", pk_dict)
                data_dict = data_dict_list[0]
                # print(f"OUTPUT {data_dict}")
                self.assertEqual(data_dict['concept_codeSystem'][0], "2.16.840.1.113883.6.1")
                self.assertEqual(data_dict['concept_code'][0], "742-7")
                self.assertEqual(data_dict['concept_id'][0], 3033575)
                self.assertEqual(data_dict['domain_id'][0], "Measurement")
            
            
            
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
                data_dict_list= parse_domain_from_dict(tree, domain, domain_meta_dict, "test_file", pk_dict)
                data_dict = data_dict_list[0]
                #print(f"OUTPUT {data_dict}")
                self.assertEqual(data_dict['attribute_value'][0], "2.16.840.1.113883.4.1")
                self.assertEqual(data_dict['text_value'][0], "2222 Home Street")

