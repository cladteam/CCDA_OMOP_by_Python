from numpy import int64
from numpy import int32
import prototype_2.value_transformations as VT

metadata = {
    'Person': {
    	# person nor patientRole have templateIDs
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Person',
            # he
    	    'element': "./hl7:recordTarget/hl7:patientRole"
    	},

        # Institution/file specific ROOT OIDs for person id named after the file in which they were discovered.
        # Often it's not clear these are legitimate or public  OIDs. I haven't found a definition for them.
        # TODO keep an eye on uniqueness and consider if our OMOP patient ID should be a concatination of
        # TODO (cont) root and extension...like if the extension is only unique within a system identified by the root.
        
       'person_id_root': {
    	    'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
    	    'attribute': "root",
           'order': 201
    	},
        'person_id_extension': {
    	    'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
    	    'attribute': "extension",
            'order':202
    	},

    	'person_id': { 
       	    'config_type': 'HASH',
            'fields' : [ 'person_id_root', 'person_id_extension' ], 
            'order' : 1
        },
        
        'gender_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:administrativeGenderCode",
    	    'attribute': "code"
    	},
    	'gender_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:administrativeGenderCode",
    	    'attribute': "codeSystem"
    	},
    	'gender_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.valueset_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'gender_concept_code',
    		    'vocabulary_oid': 'gender_concept_codeSystem',
                'default': 0
    	    },
            'order': 2
    	},

        'year_of_birth': {
            'config_type': 'DERIVED',
    	    'FUNCTION': VT.extract_year_of_birth,
    	    'argument_names': {
    		    'date_object': 'birth_datetime',
    	    },
            'order': 3
        },
        'month_of_birth': {
            'config_type': 'DERIVED',
    	    'FUNCTION': VT.extract_month_of_birth,
    	    'argument_names': {
    		    'date_object': 'birth_datetime',
    	    },
            'order': 4
        },
    	'day_of_birth': {
            'config_type': 'DERIVED',
    	    'FUNCTION': VT.extract_day_of_birth,
    	    'argument_names': {
    		    'date_object': 'birth_datetime',
    	    },
            'order': 5
    	},
    	'birth_datetime': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:patient/hl7:birthTime",
    	    'attribute': "value",
            'order': 6
    	},


    	'race_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:raceCode",
    	    'attribute': "code"
    	},
    	'race_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:raceCode",
    	    'attribute': "codeSystem"
    	},
    	'race_concept_id':{
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.valueset_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'race_concept_code',
    		    'vocabulary_oid': 'race_concept_codeSystem',
                'default': 0
    	    },
            'order': 7
    	},

    	'ethnicity_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:ethnicGroupCode",
    	    'attribute': "code"
    	},
    	'ethnicity_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:ethnicGroupCode",
    	    'attribute': "codeSystem"
    	},
    	'ethnicity_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.valueset_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'ethnicity_concept_code',
    		    'vocabulary_oid': 'ethnicity_concept_codeSystem',
                'default': 0
    	    },
            'order': 8
    	},
        
        'address_1': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:streetAddressLine',
            'attribute': "#text"
        },
        #'address_2': {
        'city': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:city',
            'attribute': "#text"
        },
        'state': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:state',
            'attribute': "#text"
        },
        'zip': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:postalCode',
            'attribute': "#text"
        },
        'location_id': { 
            'config_type': 'HASH',
            'fields' : [ 'address_1', 'city', 'state', 'zip'  ],
            'order': 9
        },

        
        'provider_id': { 'config_type': None, 'order': 10 },
        'care_site_id': { 
            'config_type': 'CONSTANT',
            'constant_value' : int64(0),
	    'order':11
        },
        'person_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':12
        },
        'gender_source_value': {
       	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:administrativeGenderCode",
    	    'attribute': "code",
            'order': 13 
         },
        'gender_source_concept_id': { 'config_type': None, 'order': 14 },
        'race_source_value': {
      	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:raceCode",
    	    'attribute': "code",
            'order': 15
         },
        'race_source_concept_id': { 'config_type': None, 'order': 16 },
        'ethnicity_source_value': {
       	    'config_type': 'FIELD',
    	    'element': "hl7:patient/hl7:ethnicGroupCode",
    	    'attribute': "code",
            'order': 17
        },
        'ethnicity_source_concept_id': { 'config_type': None, 'order': 18 },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 

    }
}
