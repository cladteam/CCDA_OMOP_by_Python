import unittest
import prototype_2.value_transformations as VT



class ValueTransformTest_codemap(unittest.TestCase):
    #2.16.840.1.113883.6.1,11579-0,3019762,Measurement
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocab_oid = '2.16.840.1.113883.6.1'
        self.concept_code = '11579-0'
        self.expected_concept_id = 3019762
        self.expected_source_concept_id = 3019762
        self.expected_domain_id = 'Measurement'  ## ????
    

    def test_codemap_xwalk_concept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, self.expected_concept_id)


    def test_codemap_xwalk_domain_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        concept_id = VT.codemap_xwalk_domain_id(args_dict)
        self.assertEqual(concept_id, self.expected_domain_id)


    def test_codemap_xwalk_default_conept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': 'bogus',
                      'default': 0 }
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, 0)


    def test_codemap_xwalk_source_conept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, self.expected_source_concept_id)


