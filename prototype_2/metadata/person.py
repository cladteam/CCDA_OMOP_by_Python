
import prototype_2.value_transformations as VT

metadata = {
    'Person': {
    	# person nor patientRole have templateIDs
    	'root': {
    	    'output': True,
    	    'config_type': 'ROOT',
    	    'element': "./recordTarget/patientRole"
    	},

        # Institution/file specific ROOT OIDs for person id named after the file in which they were discovered.
        # Often it's not clear these are legitimate or public  OIDs. I haven't found a definition for them.
        # TODO keep an eye on uniqueness and consider if our OMOP patient ID should be a concatination of
        # TODO (cont) root and extension...like if the extension is only unique within a system identified by the root.
    	'person_id': {
    	    'output': True,
            'config_type': 'PRIORITY',
            'data_type': 'INTEGER', # not applied here, go to the priority fields
            'order': 1
        },
    	'person_id_anna_flux': {
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id[@root="2.16.840.1.113883.3.651.2.1"]',
    	    'attribute': "extension",
            'priority': ('person_id', 1)
    	},
    	'person_id_patient_170': {
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id[@root="2.16.840.1.113883.3.6132"]',
    	    'attribute': "extension",
            'priority': ('person_id', 2)
    	},
    	'person_id_patient_502': {
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id[@root="2.16.840.1.113883.19.5.99999.2"]',
    	    'attribute': "extension",
            'priority': ('person_id', 3)
    	},
    	'person_id_patient_healthconnectak': {
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id[@root="2.16.840.1.113883.3.564.14977"]',
    	    'attribute': "extension",
            'priority': ('person_id', 4)
    	},
    	'person_id_patient_bennis_shauna': { # same OID as for eHx_Terry
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id[@root="2.16.840.1.113883.3.7732.100"]',
    	    'attribute': "extension",
            'priority': ('person_id', 5)
    	},
        # more general types of person Ids
    	'person_id_ssn': {
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id[@root="2.16.840.1.113883.4.1"]',
    	    'attribute': "extension",
            'priority': ('person_id', 103)
    	},
    	'person_id_npi': {
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id[@root="2.16.840.1.113883.4.6"]',
    	    'attribute': "extension",
            'priority': ('person_id', 104) # (final field name, priority number)
    	},
    	'person_id_extension_catchall': {
            # if others fail b/c they specify roots not used, just grab an extension
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id',
    	    'attribute': "extension",
            'priority': ('person_id', 105)
    	},
    	'person_id_root_catchall': {
            # if the  extension_catchall fails b/c there is no extension attribute, try just the root
    	    'output': False,
    	    'config_type': 'FIELD',
            'data_type': 'INTEGERHASH',
    	    'element': 'id',
    	    'attribute': "root",
            'priority': ('person_id', 106)
    	},
    	'person_id_hash': {
    	    'output': False,
    	    'config_type': 'HASH',
            'fields' : [ 'family_name', 'given_name', 'street_address', 'city', 'country', 'postal_code', 'gender_concept_code', 'race_concept_code', 'ethnicity_concept_code', 'date_of_birth', 'person_id_ssn', 'person_id_other' ],
            'priority': ('person_id', 107) # (final field name, priority number)
    	},

        # not for output, but to support person_id_hash creation
        'family_name': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': './recordTarget/patientRole/name/family',
    	    'attribute': "#text"
    	},
        'given_name': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': './recordTarget/patientRole/name/given',
    	    'attribute': "#text"
    	},
        'street_address': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': './recordTarget/patientRole/addr/streetAddressLine',
    	    'attribute': "#text"
    	},
        'city': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': './recordTarget/patientRole/addr/city',
    	    'attribute': "#text"
    	},
        'country': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': './recordTarget/patientRole/addr/country',
    	    'attribute': "#text"
    	},
        'postal_code': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': './recordTarget/patientRole/addr/postalCode',
    	    'attribute': "#text"
    	},

    	'gender_concept_code': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "patient/administrativeGenderCode",
    	    'attribute': "code"
    	},
    	'gender_concept_codeSystem': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "patient/administrativeGenderCode",
    	    'attribute': "codeSystem"
    	},
    	'gender_concept_id': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'gender_concept_code',
    		    'vocabulary_oid': 'gender_concept_codeSystem'
    	    },
            'order': 2
    	},

        'year_of_birth': { 'config_type': None, 'output': True,  'order': 3 },
        'month_of_birth': { 'config_type': None, 'output': True,  'order': 4 },
        'year_of_birth': { 'config_type': None, 'output': True,  'order': 5 },  ### need a function for this non-null one
    	'day_of_birth': {
    	    'output': True,
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "patient/birthTime",
    	    'attribute': "value",
            'order': 6
    	},
    	'birth_datetime': {
    	    'output': True,
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "patient/birthTime",
    	    'attribute': "value",
            'order': 7 
    	},


    	'race_concept_code': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "patient/raceCode",
    	    'attribute': "code"
    	},
    	'race_concept_codeSystem': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "patient/raceCode",
    	    'attribute': "codeSystem"
    	},
    	'race_concept_id':{
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'race_concept_code',
    		    'vocabulary_oid': 'race_concept_codeSystem'
    	    },
            'order': 8 
    	},

    	'ethnicity_concept_code': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "patient/ethnicGroupCode",
    	    'attribute': "code"
    	},
    	'ethnicity_concept_codeSystem': {
    	    'output': False,
    	    'config_type': 'FIELD',
    	    'element': "patient/ethnicGroupCode",
    	    'attribute': "codeSystem"
    	},
    	'ethnicity_concept_id': {
    	    'output': True,
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'ethnicity_concept_code',
    		'vocabulary_oid': 'ethnicity_concept_codeSystem'
    	    },
            'order': 9 
    	},

        'location_id': { 'config_type': None, 'output': True,  'order': 10 },
        'provider_id': { 'config_type': None, 'output': True,  'order': 11 },
        'care_site_id': { 'config_type': None, 'output': True,  'order': 12 },
        'person_source_value': { 'config_type': None, 'output': True,  'order': 13 },
        'gender_source_value': { 'config_type': None, 'output': True,  'order': 14 },
        'gender_source_concept_id': { 'config_type': None, 'output': True,  'order': 15 },
        'race_source_value': { 'config_type': None, 'output': True,  'order': 16 },
        'race_source_concept_id': { 'config_type': None, 'output': True,  'order': 17 },
        'ethnicity_source_value': { 'config_type': None, 'output': True,  'order': 18 },
        'ethnicity_source_concept_id': { 'config_type': None, 'output': True,  'order': 19 }
 
    }
}