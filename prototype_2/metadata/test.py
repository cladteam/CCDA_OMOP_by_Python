
import prototype_2.value_transformations as VT

metadata = {
    'Test': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Test',
    	    'element': "fake/doc/path"
        },
        
        'constant_field': {
            'config_type': 'CONSTANT',
            'constant_value': 'constant 1',
            'priority': ['value_as_number', 1],
            'order': -1
    	},

        'constant_field_2': {
            'config_type': 'CONSTANT',
            'constant_value': 'constant 2',
            'priority': ['value_as_number', 2],
            'order': 1
    	},

        'constant_oid': {
            'config_type': 'CONSTANT',
            'constant_value': '2.16.840.1.113883.6.238',
            'order': 2
    	},

        'constant_code': {
            'config_type': 'CONSTANT',
            'constant_value': '2076-8',
            'order': 3
    	},

        'field_oid': {
            'config_type': 'FIELD',
            'element': 'id',
            'attribute': 'codeSystem',
            'order': 2
    	},

        'field_code': {
            'config_type': 'FIELD',
            'element': 'id',
            'attribute': 'code',
            'order': 3
    	},

        'simple_hash_constant': {
            'config_type': 'HASH',
            'fields' : [ 'constant_field', 'constant_field_2' ],
            'order': 4 
        },
        'simple_hash_field': {
            'config_type': 'HASH',
            'fields' : [ 'field_oid', 'field_code' ],
            'order': 4 
        },

    	'test_derived_constant': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'constant_code',
    		    'vocabulary_oid': 'constant_oid',
                'default': 0
    	    },
            'order': 5 
    	},
    	'test_derived_field': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'field_code',
    		    'vocabulary_oid': 'field_oid',
                'default': 0
    	    },
            'order': 6 
    	},

        'simple_priority': {
            'config_type': 'PRIORITY',
            'order': 7
        },

        'hash_of_derived_constant': {
            'config_type': 'HASH',
            'fields' : [ 'test_derived_constant', 'test_derived_field' ],
            'order': 8
        },
        'hash_of_derived_field': {
            'config_type': 'HASH',
            'fields' : [ 'test_derived_field' ],
            'order': 9
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 
    }
}

 
