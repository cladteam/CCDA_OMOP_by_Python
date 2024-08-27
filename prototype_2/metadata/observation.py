
import prototype_2.value_transformations as VT

metadata = {
    'Observation': {
    	'root': {
    	    'output': True,
    	    'config_type': 'ROOT',
    	    'element':
    		  ("./component/structuredBody/component/section/"
    		   "templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']"
    		   "/../entry/organizer/component/observation")
    		    # FIX: another template at the observation level here: "2.16.840.1.113883.10.20.22.4.2
    		 },

    	'observation_id': {  # FIX, these IDs come up the same for all 3 observations in the CCD Ambulatory doc.
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': 'id',
    	    'attribute': 'root',   ### FIX ????
            'order': 1
    	},
    	'person_id': {
    	    'output': True,
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

    	'observation_concept_code': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "code" ,
    	    'attribute': "code"
    	},
    	'observation_concept_codeSystem': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "code",
    	    'attribute': "codeSystem"
    	},
    	'observation_concept_id': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'observation_concept_code',
    		    'vocabulary_oid': 'observation_concept_codeSystem'
    	    },
            'order': 3
    	},
    	'observation_concept_domain_id': {
    	    'output': True,
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.map_hl7_to_omop_domain_id,
    	    'argument_names': {
    		    'concept_code': 'observation_concept_code',
    		    'vocabulary_oid': 'observation_concept_codeSystem'
    	    }
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
    	'observation_date': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "effectiveTime",
            'data_type':'DATETIME',
    	    'attribute': "value",
            'order': 4
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
    	'observation_datetime': { 'output': True, 'config_type': None, 'output': True, 'order': 5 },
    	'observation_type_concept_id': { 'output': True, 'config_type': None, 'output': True, 'order': 6 },

    	'visit_occurrence_id':	{
    	    'output': True,
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id'
    	},


    	'value_as_number': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    #'FUNCTION': VT.cast_string_to_int,
    	    'FUNCTION': VT.cast_string_to_float,
    	    'argument_names': {
    		    'input': 'value_as_string',
    		    'config_type': 'value_type'
    	    },
            'order': 7
    	},
    	'value_as_string': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "value" ,
    	    'attribute': "value",
            'order': 8
    	},
    	'value_as_concept_id': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.cast_string_to_concept_id,
    	    'argument_names': {
    		    'input': 'value_as_string',
    		    'config_type': 'value_type'
    	    },
            'order': 9 
    	},
    	'value_type': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "value",
    	    'attribute': "type"
    	},
    	'value_unit':  {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': 'value',
    	    'attribute': 'unit'
    	},
        'unit_concept_id': { 'output': True, 'config_type': None, 'output': True, 'order': 10 },
        'provider_id': { 'output': True, 'config_type': None, 'output': True, 'order': 11 },
        'visit_occurence_id': { 'output': True, 'config_type': None, 'output': True, 'order': 12 },
        'visit_detail_id': { 'output': True, 'config_type': None, 'output': True, 'order': 13 },
        'observation_source_value': { 'output': True, 'config_type': None, 'output': True, 'order': 14 },
        'observation_source_concept_id': { 'output': True, 'config_type': None, 'output': True, 'order': 15 },
        'unit_source_value': { 'output': True, 'config_type': None, 'output': True, 'order': 16 },
        'qualifier_source_value': { 'output': True, 'config_type': None, 'output': True, 'order': 17 }
    }
}
