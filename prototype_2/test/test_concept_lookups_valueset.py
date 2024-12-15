import unittest
import prototype_2.value_transformations as VT


# test_data includes keys used in newer codemap and valueset calls so
# this dictionary can be passed in as args.
test_data = {
    'different domain': {
        # args
        'vocabulary_oid': "2.16.840.1.113883.6.12",
        'concept_code': "99213",
        'default':0,
        # expected outputs
        'source_concept_id': 2414397,
        'source_domain_id': 'Observation',
        'target_concept_id': 9202,   
        'target_domain_id': 'Visit'
    },
    
    'different concept': {
        # args
        'vocabulary_oid': "2.16.840.1.113883.6.96", 
        'concept_code': "266919005", 
        'default':0,
        # expected outputs
        'source_concept_id': 4144272,
        'source_domain_id': 'Observation',
        'target_concept_id': 903653, 
        'target_domain_id': 'Observation'
    },

    'same same': {
        # args
        'vocabulary_oid': "2.16.840.1.113883.6.1", 
        'concept_code': "788-0",
        'default':0,
        # expected outputs
        'source_concept_id': 3019897,
        'source_domain_id': 'Measurement',
        'target_concept_id': 3019897,
        'target_domain_id': 'Measurement'
    }
}
       


#class TestConceptLookup_map_hl7 (unittest.TestCase):
#    """ Obsolete, but this tests the original csv mapping file
#        which didn't include the final mapping!
#        So there are failures for the tests where target and source mappings differ
#    """
#    def __init__(self, *args, **kwargs):
#        super().__init__(*args, **kwargs)
#        self.first_field = None
#        self.second_field = None
#    # The criteria_map doesn't have source_concept_id
#    # def test_source_concept_id_lookup(self):        self.expected_output = ""
#
#    
#    def test_concept_id_lookup(self):
#        for test_case_key, test_case_dict in test_data.items():
#            target_concept_id = VT.map_hl7_to_omop_concept_id(test_case_dict)
#            self.assertEqual(target_concept_id, test_case_dict['target_concept_id']) # 2414397 != 9202
#        
#    def test_domain_id_lookup(self):                           
#        for test_case_key, test_case_dict in test_data.items():                         
#            target_domain_id = VT.map_hl7_to_omop_domain_id(test_case_dict)
#           self.assertEqual(target_domain_id, test_case_dict['target_domain_id']) # 'Observation' != 'Visit'
        

class TestConceptLookup_codemap (unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def test_concept_id_lookup(self):
        for test_case_key, test_case_dict in test_data.items():
            target_concept_id = VT.codemap_xwalk_concept_id(test_case_dict)
            self.assertEqual(target_concept_id, test_case_dict['target_concept_id'])
           
    def test_domain_id_lookup(self):                           
        for test_case_key, test_case_dict in test_data.items():                         
            target_domain_id = VT.codemap_xwalk_domain_id(test_case_dict)
            self.assertEqual(target_domain_id, test_case_dict['target_domain_id'])
                              
    def test_source_concept_id_lookup(self):
        for test_case_key, test_case_dict in test_data.items():                     
            source_concept_id = VT.codemap_xwalk_source_concept_id(test_case_dict)
            self.assertEqual(source_concept_id, test_case_dict['source_concept_id'])
            
            
class TestConceptLookup_value (unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
                                      
    def test_concept_id_lookup(self):
        for test_case_key, test_case_dict in test_data.items():
            target_concept_id = VT.valueset_xwalk_concept_id(test_case_dict)
            self.assertEqual(target_concept_id, test_case_dict['target_concept_id']) #
           
    def test_domain_id_lookup(self):                           
        for test_case_key, test_case_dict in test_data.items():                         
            target_domain_id = VT.valueset_xwalk_domain_id(test_case_dict)
            self.assertEqual(target_domain_id, test_case_dict['target_domain_id']) #
                              
    def test_source_concept_id_lookup(self):
        for test_case_key, test_case_dict in test_data.items():                     
            source_concept_id = VT.valueset_xwalk_source_concept_id(test_case_dict)
            self.assertEqual(source_concept_id, test_case_dict['source_concept_id']) #
