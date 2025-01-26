
import prototype_2.value_transformations as VT

metadata = {
    'Test': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Test',
    	    'element': "test elements not found in doc.s"
        },
        
        'constant_field': {
            'config_type': 'CONSTANT',
            'constant_value': 'constant 1',
            'priority': ['value_as_number', 1]
    	},

        'constant_field_2': {
            'config_type': 'CONSTANT',
            'constant_value': 'constant 2',
            'priority': ['value_as_number', 2]
    	},

        'constant_oid': {
            'config_type': 'CONSTANT',
            'constant_value': '2.16.840.1.113883.6.238',
            'order': 9999
    	},

        'constant_code': {
            'config_type': 'CONSTANT',
            'constant_value': '2076-8',
            'order': 9999
    	},

        'simple_hash': {
            'config_type': 'HASH',
            'fields' : [ 'constant_field' ],
            'order': 1
        },

    	'test_derived': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'constant_code',
    		    'vocabulary_oid': 'constant_oid',
                'default': 0
    	    },
            'order': 2
    	},

        'simple_priority': {
            'config_type': 'PRIORITY',
            'order': 3
        },

        'hash_of_derived': {
            'config_type': 'HASH',
            'fields' : [ 'test_derived' ],
            'order': 4
        }
    }
}

 
