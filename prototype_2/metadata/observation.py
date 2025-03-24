
from numpy import int32
import prototype_2.value_transformations as VT

metadata = {
    'Observation': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Observation',
            # Results section
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.3.1'  or @root='2.16.840.1.113883.10.20.22.2.3']"
    		   "/../hl7:entry/hl7:organizer/hl7:component/hl7:observation")
    		 },

    	'observation_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'root',
            'order': 1001
    	},       
    	'observation_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'extension',
            'order': 1002
    	},
    	'observation_id_hash': {
    	    'config_type': 'HASH',
            'fields' : [ 'observation_id_extension', 'observation_id_root' ], 
            'priority': ('observation_id', 1)
    	},
    	'observation_id_constant': {
            'config_type': 'CONSTANT',
            'constant_value' : 999,
            'priority': ('observation_id', 100)
        },
    	'observation_id_field_hash': {
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'visit_occurrence_id', 'observation_concept_id', 'observation_time',
                    'value_as_string', 'value_as_nmber', 'value_as_concept_id'],
            'priority': ('observation_id', 2)
    	},
        'observation_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },


    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

    	'observation_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code" ,
    	    'attribute': "code"
    	},
    	'observation_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code",
    	    'attribute': "codeSystem"
    	},
    	'observation_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'observation_concept_code',
    		    'vocabulary_oid': 'observation_concept_codeSystem',
                'default': 0
    	    },
            'order': 3
    	},

    	'observation_concept_domain_id': {
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'observation_concept_code',
    		    'vocabulary_oid': 'observation_concept_codeSystem',
                'default': 'n/a'
    	    }
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
    	'observation_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'order': 4
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
    	'observation_datetime': { 'config_type': None, 'order': 5 },
    	'observation_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32827),
            'order': 6
        },

    	'visit_occurrence_id':	{
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id'
    	},

    	'value_type': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:value",
    	    'attribute': "{http://www.w3.org/2001/XMLSchema-instance}type",
    	},


    	'value_as_string': {
    	    'config_type': 'FIELD',
            'element': 'hl7:value[@xsi:type="ST"]' , # TODO TEST these
    	    'attribute': "#text",
            'order': 7
    	},


    	'value_as_number': {
    	    'config_type': 'FIELD',
            'data_type':'FLOAT',
    	    'element': 'hl7:value[@xsi:type="PQ"]' ,
    	    'attribute': "value",
            'order':8
    	},


    	'value_as_code_CD': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value[@xsi:type="CD"]' ,
    	    'attribute': "code",
        },
    	'value_as_codeSystem_CD': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value[@xsi:type="CD"]' ,
    	    'attribute': "codeSystem",
        },
    	'value_as_concept_id_CD': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'value_as_code_CD',
    		    'vocabulary_oid': 'value_as_codeSystem_CD',
                'default': None
            },
            'priority': ['value_as_concept_id', 2]
    	},
    	'value_as_code_CE': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value[@xsi:type="CE"]' ,
    	    'attribute': "code",
        },
    	'value_as_codeSystem_CE': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value[@xsi:type="CE"]' ,
    	    'attribute': "codeSystem",
        },
    	'value_as_concept_id_CE': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'value_as_code_CE',
    		    'vocabulary_oid': 'value_as_codeSystem_CE',
                'default': None
            },
            'priority': ['value_as_concept_id', 1]
    	},
        'value_as_concept_id_na': {
            'config_type': 'CONSTANT',
            'constant_value' : 0,
            'priority': ['value_as_concept_id', 100]
        },
    	'value_as_concept_id': {
    	    'config_type': 'PRIORITY',
            'order':  9
    	},



        'value_unit':  {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value',
    	    'attribute': 'unit'
    	},
        'qualifier_concept_id' : { 'config_type': None, 'order': 10 },
        'unit_concept_id': { 'config_type': None, 'order': 11 },
        'provider_id': { 'config_type': None, 'order': 12 },
        'visit_occurrence_id': { 'config_type': None, 'order': 13 },
        'visit_detail_id': { 'config_type': None, 'order': 14 },

        'observation_source_value': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,
    	    'argument_names': {
    		    'first_field': 'observation_concept_code',
    		    'second_field': 'observation_concept_codeSystem',
                'default': 0
    	    },
            'order' : 15
    	},

        'observation_source_concept_id': { 'config_type': None, 'order': 16 },

        'unit_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order': 17 
        },
        'qualifier_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order': 18 
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 
    }
}





