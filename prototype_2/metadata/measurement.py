
import prototype_2.value_transformations as VT

metadata = {
    'Measurement': {
    	'root': {
    	    'output': True,
    	    'config_type': 'ROOT',
    	    'element':
    		  ("./component/structuredBody/component/section/"
    		   "templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']"
    		   "/../entry/organizer/component/observation")
    		    # FIX: another template at the observation level here: "2.16.840.1.113883.10.20.22.4.2
        },

    	'measurement_id_basic': {  # FIX, these IDs come up the same for all 3 observations in the CCD Ambulatory doc.
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': 'id',
    	    'attribute': 'root',
           'priority': ('measurement_id', 1)
    	},
    	'measurement_id_hash': {
    	    'output': True,
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'visit_occurrence_id', 'measurement_concept_id', 'measurement_time', 'value_as_string'],
            'priority': ('measurement_id', 100)
    	},
        'measurement_id': {
            'output':True,
            'config_type': 'PRIORITY',
            'order': 1
        },

    	'person_id': {
    	    'output': True,
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

    	'measurement_concept_code': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "code" ,
    	    'attribute': "code"
    	},
    	'measurement_concept_codeSystem': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "code",
    	    'attribute': "codeSystem"
    	},
    	'measurement_concept_id': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'measurement_concept_code',
    		    'vocabulary_oid': 'measurement_concept_codeSystem'
    	    },
            'order': 3
    	},

    	'measurement_concept_domain_id': {
    	    'output': True,
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.map_hl7_to_omop_domain_id,
    	    'argument_names': {
    		    'concept_code': 'measurement_concept_code',
    		    'vocabulary_oid': 'measurement_concept_codeSystem'
    	    }
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
        'measurement_date': {
    	    'output': True,
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "effectiveTime",
    	    'attribute': "value",
            'order': 4
    	},
        'measurement_datetime': { 'output': True, 'config_type': None, 'output': True, 'order': 5 },
        'measurement_time': { 'output': True, 'config_type': None, 'output': True,  'order': 6 },
        'measurement_type_concept_id': { 'output': True, 'config_type': None, 'output': True,  'order': 7 },
        'operator_concept_id': { 'output': True, 'config_type': None, 'output': True,  'order': 8 },

    	'value_as_string': {
    	    'output': True,
    	    'config_type': 'FIELD',
    	    'element': "value" ,
    	    'attribute': "value",
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
            'order': 9
    	},
    	'value_as_concept_id': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.cast_string_to_concept_id,
    	    'argument_names': {
    		    'input': 'value_as_string',
    		    'config_type': 'value_type'
    	    },
            'order':  10
    	},

    	'unit_concept_id': { 'output': True, 'config_type': None, 'output': True,  'order':  11 },
    	'provider_id': { 'output': True, 'config_type': None, 'output': True,  'order':  12 },

    	'visit_occurrence_id':	{
    	    'output': True,
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order':  13
    	},
    	'visit_detail_id':	{ 'output': True, 'config_type': None, 'output': True,  'order':  14 },
    	'measurement_source_value':	{ 'output': True, 'config_type': None, 'output': True,  'order':  15 },
    	'measurement_source_concept_id':	{ 'output': True, 'config_type': None, 'output': True,  'order':  16 },
    	'unit_source_value':	{ 'output': True, 'config_type': None, 'output': True,  'order':  17 },
    	'value_source_value':	{ 'output': True, 'config_type': None, 'output': True,  'order':  18 }
    }
}
