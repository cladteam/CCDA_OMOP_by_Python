from numpy import int32
import prototype_2.value_transformations as VT
# converted to encouter from encompassingEncounter. The encompassingEncounter attributes are commented out
metadata = {
    'Visit_encompassingEncounter': {
    	# FIX: there's a code for what might be admitting diagnosis here
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Visit',
            # encompassingEncounter in header
    	    'element': './hl7:componentOf/hl7:encompassingEncounter'
    	},
        
        'visit_occurrence_id_root': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:id',
    	    'attribute': "root"
    	},
        'visit_occurrence_id_extension': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:id',
    	    'attribute': "extension"
    	},
    	'visit_occurrence_id': { 
       	    'config_type': 'HASH',
            'fields' : [ 'visit_occurrence_id_root', 'visit_occurrence_id_extension' ], 
            'order' : 1
        },

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

    	'visit_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code",	 # FIX ToDo is this what I think it is?, see #72 etc.
    	    'attribute': "code"
    	},
    	'visit_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code",
    	    'attribute': "codeSystem"
    	},
    	'visit_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.visit_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'visit_concept_code',
    		    'vocabulary_oid': 'visit_concept_codeSystem',
                'default': 0
    	    },
            'order': 3
    	},

    	'visit_start_date': {
    	    'config_type': 'PRIORITY',
            'order':4
    	},
    	'visit_start_date_low': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime/hl7:low[not(@nullFlavor=\"UNK\")]",
    	    'attribute': "value",
            'priority':  ['visit_start_date', 1]
    	},
    	'visit_start_date_value': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'priority':  ['visit_start_date', 2]
    	},
    	'visit_start_datetime': {
    	    'config_type': 'PRIORITY',
            'order':5
    	},
    	'visit_start_datetime_low': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime/hl7:low[not(@nullFlavor=\"UNK\")]",
    	    'attribute': "value",
            'priority':  ['visit_start_datetime', 1]
    	},
    	'visit_start_datetime_value': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'priority':  ['visit_start_datetime', 2]
    	},




    	'visit_end_date':  {
    	    'config_type': 'PRIORITY',
            'order':6
    	},
    	'visit_end_date_high':  {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
            'element': "hl7:effectiveTime/hl7:high[not(@nullFlavor=\"UNK\")]",
    	    'attribute': "value",
            'priority': ['visit_end_date', 1]
    	},
    	'visit_end_date_value': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'priority':  ['visit_end_date', 2]
    	},
        # too loose! too forgiving? If the high value is UNK, this
        # will use the low date. See #211 for options.
    	'visit_end_date_low': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime/hl7:low",
    	    'attribute': "value",
            'priority':  ['visit_end_date', 3]
    	},

    	'visit_end_datetime':  {
    	    'config_type': 'PRIORITY',
            'order':7
    	},
        
    	'visit_end_datetime_high':  {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
            'element': "hl7:effectiveTime/hl7:high[not(@nullFlavor=\"UNK\")]",
    	    'attribute': "value",
            'priority': ['visit_end_datetime', 1]
    	},
    	'visit_end_datetime_value': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'priority':  ['visit_end_datetime', 2]
    	},
        # too loose! too forgiving? If the high value is UNK, this
        # will use the low date. See #211 for options.
    	'visit_end_datetime_low': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime/hl7:low",
    	    'attribute': "value",
            'priority':  ['visit_end_datetime', 3]
    	},

        'visit_type_concept_id' : {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32827),
            'order': 8
        },

    	# FIX TODO sometimes a document will have more than one encounterParticipant. 
        # The way this is configured, they will be awkwardly merged.
        # (missing Ex. documents) This might be mitigated by the addition of SDLOC on care_site which
        # is where this is used. The providers are performers, not participants.
        
    	'provider_id': {
    	    'config_type': 'PRIORITY',
            'order': 9
    	},
        
    	'provider_id_performer_root': {
    	    'config_type': 'FIELD',
    	    'element':'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:id', 
    	    'attribute': "root",
    	},
    	'provider_id_performer_extension': {
    	    'config_type': 'FIELD',
    	    'element':'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:id', 
    	    'attribute': "extension",
    	},
    	'provider_id_performer': {
    	    'config_type': 'HASH',
            'fields' : ['provider_id_performer_root', 'provider_id_performer_extension'],
            'priority': ['provider_id', 1]
    	},
        
        ## TODO FIX, is this address under the perfomer or here in the encounter? Without an "order" attribute to output this, 
        # the field could be null when used in the hash and not noticed. 
        # TODO FIX how to test this without a plethora of extra columns? Have the has function throw errors on null fields?
        'provider_id_street': {
            'config_type': 'FIELD',
            'element': 'hl7:encounterParticipant/hl7:assignedEntity/hl7:addr/hl7:streetAddressLine',  # noqa: E501
            'attribute': "#text"
        },
        'provider_id_city': {
            'config_type': 'FIELD',
            'element': 'hl7:encounterParticipant[@typeCode=”ATND”]/hl7:assignedEntity/hl7:addr/hl7:city',  # noqa: E501
            'attribute': "#text"
        },
        'provider_id_state': {
            'config_type': 'FIELD',
            'element': 'hl7:encounterParticipant[@typeCode=”ATND”]/hl7:assignedEntity/hl7:addr/hl7:state',
            'attribute': "#text"
        },
        'provider_id_zip': {
            'config_type': 'FIELD',
            'element': 'hl7:encounterParticipant[@typeCode=”ATND”]/hl7:assignedEntity/hl7:addr/hl7:postalCode',
            'attribute': "#text"
        },
        'provider_id_given': {
            'config_type': 'FIELD',
            'element': 'hl7:encounterParticipant[@typeCode=”ATND”]/hl7:assignedEntity/hl7:assignedPerson/hl7:name/hl7:given',  # noqa: E501
            'attribute': "#text"
        }, 

        'provider_id_family': {
            'config_type': 'FIELD',
            'element': 'hl7:encounterParticipant[@typeCode=”ATND”]/hl7:assignedEntity/hl7:assignedPerson/hl7:name/hl7:family',  # noqa: E501
            'attribute': "#text"
        },
        'provider_id_hash': {
            'config_type': 'HASH',
            'fields' : [ 'provider_id_street', 'provider_id_city', 'provider_id_state', 
                         'provider_id_zip', 'provider_id_given', 'provider_id_family'],
            'priority' : ['provider_id', 2]
        },


        
    	'care_site_id': {
    	    'config_type': 'FIELD',
            'data_type': 'LONG',
            'element': 'hl7:location/hl7:healthCareFacility/hl7:location/hl7:addr', 
            #'element': "participant/participantRole[@classCode="SDLOC"]/playingEntity", 
    	    #'element': "hl7:location/hl7:healthCareFacility/hl7:id",
    	    'attribute': "root",
            'order': 10
    	},

        'visit_source_value': { 
            'config_type': 'DERIVED', 
            'FUNCTION': VT.concat_fields,
    	    'argument_names': {
                'first_field': 'visit_concept_codeSystem',
    		    'second_field': 'visit_concept_code',
                'default': 0
    	    },
            'order': 11
        },
        
        'visit_source_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.visit_xwalk_source_concept_id,
    	    'argument_names': {
    		    'concept_code': 'visit_concept_code',
    		    'vocabulary_oid': 'visit_concept_codeSystem',
                'default': 0
    	    },
            'order': 12 },
        'admitting_source_concept_id': { 'config_type': None, 'order': 13},
        'admitting_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':41
        },
        'discharge_to_concept_id': { 'config_type': None, 'order': 15},
        'discharge_to_source_value':  {
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':16
        },
        'preceding_visit_occurrence_id': { 'config_type': None, 'order': 17},

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 
        
        # 'custodian_id' : {
        #     'config_type': 'PRIORITY',
        #     'orider': 18
        # }
        #,'custodian_id_constant' : {
        #    'config_type': 'CONSTANT_PK', # new type
        #    'constant_value': "unknown", # place holder
        #    'priority' : ['custodian_id', 100]
        #}
        #, 'custodian_id_field_ext' : {
        #    'config_type': 'FIELD',
        #    'element_no_root': "./custodian/id",  # new attribute, conditional on lack of just 'element'
        #    'attribute' : 'extension',
        #    'priority' : ['custodian_id', 1]  
        #  }
        #, 'custodian_id_field_root' : {
        #    'config_type': 'FIELD',
        #    'element_no_root': "./custodian/id", 
        #    'attribute' : 'extension',
        #    'priority' : ['custodian_id' 2]
        #  }
        # TODO: add custodian_id to person, and as a FK to the domains
    }
}
