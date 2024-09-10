
import prototype_2.value_transformations as VT

metadata = {
    'Measurement': {
    	'root': {
    	    'config_type': 'ROOT',
    	    'element':
    		  ("./component/structuredBody/component/section/"
    		   "templateId[@root='2.16.840.1.113883.10.20.22.2.3.1']"
    		   "/../entry/organizer/component/observation")
    		    # FIX: another template at the observation level here: "2.16.840.1.113883.10.20.22.4.2
        },

    	'measurement_id_basic': {  # FIX, these IDs come up the same for all 3 observations in the CCD Ambulatory doc.
    	    'config_type': 'FIELD',
    	    'element': 'id',
    	    'attribute': 'root',
           'priority': ('measurement_id', 9999) # rejected, last in priority
    	},
    	'measurement_id_hash': {
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'visit_occurrence_id', 'measurement_concept_id', 'measurement_time', 'value_as_string'],
            'priority': ('measurement_id', 100)
    	},
        'measurement_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

    	'measurement_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "code" ,
    	    'attribute': "code"
    	},
    	'measurement_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "code",
    	    'attribute': "codeSystem"
    	},
    	'measurement_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'measurement_concept_code',
    		    'vocabulary_oid': 'measurement_concept_codeSystem',
                'default': (0,'default')
    	    },
            'order': 3
    	},

    	'measurement_concept_domain_id': {
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.map_hl7_to_omop_domain_id,
    	    'argument_names': {
    		    'concept_code': 'measurement_concept_code',
    		    'vocabulary_oid': 'measurement_concept_codeSystem',
                'default': (0,'default')
    	    }
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
        'measurement_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "effectiveTime",
    	    'attribute': "value",
            'order': 4
    	},
        'measurement_datetime': { 'config_type': None, 'order': 5 },
        'measurement_time': { 'config_type': None, 'order': 6 },
        'measurement_type_concept_id': { 'config_type': None, 'order': 7 },
        'operator_concept_id': { 'config_type': None, 'order': 8 },

        # This is heinous. We don't need value_as_string in measurement, but as input to cast to value_as_number.
        # IIRC the code currently treats priority fields as coming after DERIVED values, and here they
        # would be input to one.
        # FIX https://github.com/cladteam/CCDA_OMOP_by_Python/issues/77 #77
    	#'value_as_string_text': {
        #    'config_type': 'FIELD',
    	#    'element': "value" ,
    	#    'attribute': "#text",
        #    'priority' : ['value_as_string', 2]
    	#},
    	#'value_as_string_value': {
    	#    'config_type': 'FIELD',
    	#    'element': "value" ,
    	#    'attribute': "value",
        #    'priority' : ['value_as_string', 1]
    	#},
        # This is so value_as_number has something to work with in some cases.
        # In other cases, this will fail as described above and in #77
        # and yes, this must conflict with the priority group above in some way.
    	'value_type': {
    	    'config_type': 'FIELD',
    	    'element': "value",
    	    'attribute': "{http://www.w3.org/2001/XMLSchema-instance}type",
#            'order': 100
    	},
    	'value_as_string': {
    	    'config_type': 'FIELD',
    	    'element': 'value[@{http://www.w3.org/2001/XMLSchema-instance}type="ST"]' ,
    	    'attribute': "#text",
    	},
    	'value_as_number': {
    	    'config_type': 'FIELD',
    	    'element': 'value[@{http://www.w3.org/2001/XMLSchema-instance}type="PQ"]' ,
    	    'attribute': "value",
            'order': 9
    	},
    	'value_as_code': {
    	    'config_type': 'FIELD',
    	    'element': 'value[@{http://www.w3.org/2001/XMLSchema-instance}type="CD"]' ,
    	    'attribute': "code",
#            'order' : 130
        },
    	'value_as_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': 'value[@{http://www.w3.org/2001/XMLSchema-instance}type="CD"]' ,
    	    'attribute': "codeSystem",
#            'order' : 131
        },
    	'value_as_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'value_as_code',
    		    'vocabulary_oid': 'valuse_as_codeSystem',
                'default': (0,'default')
            },
            'order':  10
    	},

    	'unit_concept_id': { 'config_type': None, 'order':  11 },
    	'provider_id': { 'config_type': None, 'order':  12 },

    	'visit_occurrence_id':	{
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order':  13
    	},
    	'visit_detail_id':	{ 'config_type': None, 'order':  14 },

    	'measurement_source_value':	{ 'config_type': None, 'order':  15 },
    	'measurement_source_concept_id':	{ 'config_type': None, 'order':  16 },

    	'unit_source_value':	{ 'config_type': None, 'order':  17 },

    	'value_source_value_constant': {
    	    'config_type': 'CONSTANT',
            'constant_value': 'n/a',
            'priority': ['value_source_value', 4],
#            'order': 123
        },
    	'value_source_value_text': {
    	    'config_type': 'FIELD',
    	    'element': 'value[@{http://www.w3.org/2001/XMLSchema-instance}type="ST"]' ,
    	    'attribute': "#text",
            'priority': ['value_source_value', 3],
#            'order': 120
        },
    	'value_source_value_code': {
    	    'config_type': 'FIELD',
    	    'element': 'value[@{http://www.w3.org/2001/XMLSchema-instance}type="CD"]' ,
    	    'attribute': "code",
            'priority': ['value_source_value', 2],
#            'order': 121
        },
    	'value_source_value_value': {
    	    'config_type': 'FIELD',
    	    'element': 'value[@{http://www.w3.org/2001/XMLSchema-instance}type="PQ"]' ,
    	    'attribute': "value",
            'priority': ['value_source_value', 1],
#            'order': 122
        },
        'value_source_value' : {
            'config_type': 'PRIORITY',
            'order':18
        }
    }
}
