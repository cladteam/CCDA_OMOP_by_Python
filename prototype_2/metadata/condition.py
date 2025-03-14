
import prototype_2.value_transformations as VT
from numpy import int32

metadata = {
    'Condition': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Condition',
            # Problems Sections
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.5' or  @root='2.16.840.1.113883.10.20.22.2.5.1' ] "
    		   "/../hl7:entry/hl7:act/hl7:entryRelationship/hl7:observation")
        },

    	'condition_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'root',
            'order': 1001
    	},
    	'condition_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'extension',
            'order': 1002
    	},
    	'condition_id_hash': {
    	    'config_type': 'HASH',
            'fields' : [ 'condition_id_extension', 'condition_id_root' ],
            'priority': ('condition_occurrence_id', 1)
    	},
    	'condition_id_constant': {
            'config_type': 'CONSTANT',
            'constant_value' : 999,
            'priority': ('condition_occurrence_id', 100)
        },
    	'condition_id_field_hash': {
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'visit_occurrence_id', 'condition_concept_id', 'condition_time',
                    'value_as_string', 'value_as_nmber', 'value_as_concept_id'],
            'priority': ('condition_occurrence_id', 2)
    	},
        'condition_occurrence_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

        # <code code="8029-1" codeSystem="1232.23.3.34.3..34"> 
    	'condition_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:value" ,
    	    'attribute': "code"
    	},
    	'condition_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:value",
    	    'attribute': "codeSystem"
    	},
    	'condition_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'condition_concept_code',
    		    'vocabulary_oid': 'condition_concept_codeSystem',
                'default': 0
    	    },
            'order': 3
    	},

        'condition_start_date': {  #20081022, #200810221850-0400
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime/hl7:low",
    	    'attribute': "value",
            'order': 4
    	},
        'condition_start_datetime': { 
           	'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime/hl7:low",
    	    'attribute': "value",
            'order': 5
    	},
        'condition_end_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime/hl7:high",
    	    'attribute': "value",
            'order': 6
    	},
        'condition_end_datetime': { 
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime/hl7:high",
    	    'attribute': "value",
            'order': 7
    	},
        'condition_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32840), 
            'order': 8
        },
        'condition_status_concept_id': {
            'config_type': None,
            'order': 9
        },
        'stop_reason': { 
            'config_type': 'CONSTANT',
            'constant_value' : '', 
            'order': 10
        },
        'provider_id': { 
            'config_type': 'FK', 
            'FK': 'provider_id',
            'order': 11 
        }, 
        'visit_occurrence_id': { 
            'config_type': 'FK', 
            'FK': 'visit_occurrence_id',
            'order': 12 
        },
        'visit_detail_id': { 'config_type': None, 'order': 13 },  # n/a
        'condition_source_value': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names':{
                'first_field': 'condition_concept_code',
                'second_field': 'condition_concept_codeSystem',
                'default' : 'error'
            },
            'order': 14
        },
        'condition_source_concept_id': { 'config_type': None, 'order': 15 },
        'condition_status_source_value': {
            'config_type': 'CONSTANT',
            'constant_value' : '', 
            'order': 16
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	}

    }
}
