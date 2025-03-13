
from numpy import int32
import prototype_2.value_transformations as VT
#
metadata = {
    'Procedure_activity_procedure': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Procedure',
            # Procedure section, entry, procedure
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.7' or @root='2.16.840.1.113883.10.20.22.2.7.1']"
    		   "/../hl7:entry/hl7:procedure[@moodCode='EVN']/"
               "hl7:statusCode[@code='active' or @code='completed']/..")
        },

    	'procedure_occurrence_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'root',
            'order': 1001
    	},
    	'procedure_occurrence_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'extension',
            'order': 1002
    	},
    	'procedure_occurrence_id_hash': {
    	    'config_type': 'HASH',
            'fields' : [ 'procedure_occurrence_id_root', 'procedure_occurrence_id_extension' ],
            'priority': ('procedure_occurrence_id', 1)
    	},
    	'procedure_occurrence_id_field_hash': {
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'visit_occurrence_id', 'procedure_concept_id', 'procedure_datetime'],
            'priority': ('procedure_occurrence_id', 2)
    	},
        'procedure_occurrence_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

    	'procedure_concept_id_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code" ,
    	    'attribute': "code"
    	},
    	'procedure_concept_id_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code",
    	    'attribute': "codeSystem"
    	},
    	'procedure_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,  
    	    'argument_names': {
    		    'concept_code': 'procedure_concept_id_code',
    		    'vocabulary_oid': 'procedure_concept_id_codeSystem',
                'default': 0
            },
            'order': 3
    	},

    	'procedure_concept_domain_id': {
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'procedure_concept_id_code',
    		    'vocabulary_oid': 'procedure_concept_id_codeSystem',
                'default': 0
    	    }
    	},           
        
        'procedure_date': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATE',
            'order': 4
        },        

        'procedure_datetime': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATETIME',
            'order': 5
        },        

        'procedure_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32817), # OMOP concept ID for 'EHR'
            'order': 6
        },
        
        'modifier_concept_id': { 'config_type': None, 'order': 7 },
        'quantity': { 'config_type': None, 'order': 8},
        
        'provider_id': { 
    	    'config_type': 'FK',
    	    'FK': 'provider_id',
            'order': 9
    	},

        'visit_occurrence_id': {
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order': 10
    	},     
        
        'visit_detail_id': { 'config_type': None, 'order': 11 },

        'procedure_source_value': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,  
    	    'argument_names': {
    		    'first_field': 'procedure_concept_id_code',
    		    'second_field': 'procedure_concept_id_codeSystem',
                'default': 'error'
    	    },
            'order': 12
        },

        'procedure_source_concept_id': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_source_concept_id,  
            'argument_names': {
                'concept_code': 'procedure_concept_id_code',
                'vocabulary_oid': 'procedure_concept_id_codeSystem',
                'default': 0
            },
            'order': 13
        },
        
        'modifier_source_value': {
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order': 14
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 

    }
}
