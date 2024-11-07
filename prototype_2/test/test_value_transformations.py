import unittest
import prototype_2.value_transformations as VT


class ValueTransformTest_map_to_standard_1(unittest.TestCase):
    """ this tests an older csv file
    """
    #2.16.840.1.113883.6.1,11579-0,3019762,Measurement
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocab_oid = '2.16.840.1.113883.5.1'
        self.concept_code = 'F'
        self.expected_concept_id = 8532
        self.expected_source_concept_id = 0
        self.expected_domain_id = 'Gender'

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
        args_dict = { 'vocabulary_oid': self.vocab_oid, 'concept_code': 'bogus', 'default': 0 }
        concept_id = VT.map_hl7_to_omop_concept_id(args_dict)
        self.assertEqual(concept_id, 0) 


class ValueTransformTest_map_to_standard_2(unittest.TestCase):
    """ this tests an older csv file
    """
    #2.16.840.1.113883.6.1,11579-0,3019762,Measurement
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocab_oid = '2.16.840.1.113883.6.1'
        self.concept_code = '11579-0'
        self.expected_concept_id = 3019762
        self.expected_source_concept_id = 3019762
        self.expected_domain_id = 'Measurement'  ## ????

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
        args_dict = { 'vocabulary_oid': self.vocab_oid, 'concept_code': 'bogus', 'default': 0 }
        concept_id = VT.map_hl7_to_omop_concept_id(args_dict)
        self.assertEqual(concept_id, 0) 
