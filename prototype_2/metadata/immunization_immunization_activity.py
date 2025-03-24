
import prototype_2.value_transformations as VT
from numpy import int32
#
metadata = {
    'Immunization_immunization_activity': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Drug',
            # Immunizations section, entry, substanceAdministration
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.2' or @root='2.16.840.1.113883.10.20.22.2.2.1']"
    		   "/../hl7:entry/hl7:substanceAdministration[@moodCode='EVN']/"
               "hl7:statusCode[@code='active' or @code='completed']/..")
            
#    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
#    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.2' or @root='2.16.840.1.113883.10.20.22.2.2.1']"
#    		   "/../hl7:entry/hl7:substanceAdministration[@moodCode='EVN' and "
#               "(hl7:statusCode[@code='active'] or hl7:statusCode[@code='completed'])]")
        },

    	'drug_exposure_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'root',
            'order': 1001
    	},
    	'drug_exposure_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'extension',
            'order': 1002
    	},
    	'drug_exposure_id_hash': {
    	    'config_type': 'HASH',
            'fields' : [ 'drug_exposure_id_root', 'drug_exposure_id_extension' ],
            'priority': ('drug_exposure_id', 1)
    	},
    	'drug_exposure_id_field_hash': {
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'visit_occurrence_id', 'drug_ex_concept_id', 'drug_exposure_time',
                    'value_as_string', 'value_as_nmber', 'value_as_concept_id'],
            'priority': ('drug_exposure_id', 2)
    	},
        'drug_exposure_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

        # consumable/manufacturedProduct/manufacturedMaterial/...
    	'drug_concept_code_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:consumable/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:code" ,
    	    'attribute': "code"
    	},
    	'drug_concept_codeSystem_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:consumable/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:code",
    	    'attribute': "codeSystem"
    	},
    	'drug_concept_id_code': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,  
    	    'argument_names': {
    		    'concept_code': 'drug_concept_code_code',
    		    'vocabulary_oid': 'drug_concept_codeSystem_code',
                'default': 0
            },
            'priority': ('drug_concept_id', 1)
    	},

    	'drug_concept_domain_id_code': {
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'drug_concept_code_code',
    		    'vocabulary_oid': 'drug_concept_codeSystem_code',
                'default': 0
    	    }
    	},

    	'drug_concept_code_translation': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:consumable/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:code/hl7:translation" ,
    	    'attribute': "code"
    	},
    	'drug_concept_codeSystem_translation': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:consumable/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:code/hl7:translation",
    	    'attribute': "codeSystem"
    	},
    	'drug_concept_id_translation': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,  
    	    'argument_names': {
    		    'concept_code': 'drug_concept_code_translation',
    		    'vocabulary_oid': 'drug_concept_codeSystem_translation',
                'default': 0
    	    },
            'priority': ('drug_concept_id', 2)
    	},

    	'drug_concept_domain_id_translation': {
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'drug_concept_code_translation',
    		    'vocabulary_oid': 'drug_concept_codeSystem_translation',
                'default': 0
    	    }
    	},
        
        'drug_concept_id': {
            'config_type': 'PRIORITY',
            'order' : 3
        },                  
        
        'drug_exposure_start_date': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATE',
            'order': 4
        },        

        'drug_exposure_start_datetime': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATETIME',
            'order': 5
        },        

        'drug_exposure_end_date': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATE',
            'order': 6
        },        

        'drug_exposure_end_datetime': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATETIME',
            'order': 7
        },        
       
        'verbatim_end_date': {
    	    'config_type': 'FIELD',
            'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'data_type':'DATE',
            'order': 8
    	},        
        
        'drug_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32818), # OMOP concept ID for 'EHR administration record'
            'order': 9
        },
        
        'stop_reason': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order':10
        },
        'refills': { 'config_type': None, 'order': 11},
       
        'quantity': {
            'config_type': 'FIELD',
            'element': "hl7:doseQuantity",
            'attribute': "value",
            'data_type': 'FLOAT',
            'order': 12
        },
        
        'days_supply': { 'config_type': None, 'order': 13 },
        'sig': { 'config_type': None, 'order': 14 },

        'route_concept_code': {
            'config_type': 'FIELD',
            'element': "hl7:routeCode",
            'attribute': "code"
        },
        'route_concept_codeSystem': {
            'config_type': 'FIELD',
            'element': "hl7:routeCode",
            'attribute': "codeSystem"
        },
        'route_concept_id': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_concept_id,  
            'argument_names': {
                'concept_code': 'route_concept_code',
                'vocabulary_oid': 'route_concept_codeSystem',
                'default': 0
            },
            'order': 15
        },

        # consumable/manufacturedProduct/manufacturedMaterial/lotNumberText
        'lot_number': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:consumable/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:lotNumberText",
            'data_type': 'TEXT',
            'attribute': '#text',
            'order': 16
        },
        
        'provider_id': { 
    	    'config_type': 'FK',
    	    'FK': 'provider_id',
            'order': 17
    	},

        'visit_occurrence_id': {
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order':  18
    	},
        
        'visit_detail_id': { 'config_type': None, 'order': 19 },

        'drug_source_value_translation': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,  
    	    'argument_names': {
    		    'first_field': 'drug_concept_code_translation',
    		    'second_field': 'drug_concept_codeSystem_translation',
                'default': 'error'
    	    },
            'priority': ( 'drug_source_value', 2)
        },
        'drug_source_value_code': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,  
    	    'argument_names': {
    		    'first_field': 'drug_concept_code_code',
    		    'second_field': 'drug_concept_codeSystem_code',
                'default': 'error'
    	    },
            'priority': ( 'drug_source_value', 1)
        },
        'drug_source_value': {
            'config_type': 'PRIORITY',
            'order': 20
        },
        
        'drug_source_concept_id_translation': { 
            'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_source_concept_id,  
            'argument_names': {
                'concept_code': 'drug_concept_code_translation',
                'vocabulary_oid': 'drug_concept_codeSystem_translation',
                'default': 0
            },
            'priority' : ('drug_source_concept_id', 2)
        },
        'drug_source_concept_id_code': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_source_concept_id,  
            'argument_names': {
                'concept_code': 'drug_concept_code_code',
                'vocabulary_oid': 'drug_concept_codeSystem_code',
                'default': 0
            },
            'priority' : ('drug_source_concept_id', 1)
        },
        'drug_source_concept_id': {
            'config_type': 'PRIORITY',
            'order': 21
        },
        
        'route_source_value': { # CHRIS very nice
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,  
    	    'argument_names': {
    		    'first_field': 'route_concept_code',
    		    'second_field': 'route_concept_codeSystem',
                'default': 'error'
    	    },
            'order': 22
        },
               
        'dose_unit_source_value': { 
       	    'config_type': 'FIELD',
            'element': "hl7:doseQuantity",
            'attribute': "unit",
            'order': 23
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 

    }
}
