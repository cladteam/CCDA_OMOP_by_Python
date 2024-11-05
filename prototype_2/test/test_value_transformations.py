import unittest
import prototype_2.value_transformations as VT


class ValueTransformTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocab_oid = '2.16.840.1.113883.5.1'
        self.concept_code = 'F'
        self.expected_concept_id = 8532
        self.expected_domain_id = 'Gender'
    
    def test_always_good(self):
        self.assertTrue(True)

    def test_map_hl7_to_omop_concept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        concept_id = VT.map_hl7_to_omop_concept_id(args_dict)
        self.assertEqual(concept_id, self.expected_concept_id)
                         
    def test_map_hl7_to_omop_domain_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        domain_id = VT.map_hl7_to_omop_domain_id(args_dict)
        self.assertEqual(domain_id, self.expected_domain_id)


    def test_map_default_concept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': 'bogus',
                      'default': 0 }
        concept_id = VT.map_hl7_to_omop_concept_id(args_dict)
        self.assertEqual(concept_id, 0) 
