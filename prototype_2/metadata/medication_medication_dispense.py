
from numpy import int32
import prototype_2.value_transformations as VT

metadata = {
    'Medication_medication_dispense': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Drug',
            # Medication Dispense
            # Medications section, entry, substanceAdministration, entryRelationship, supply
            #substanceAdministration/@moodCode="INT" represents intention of patient to take the  medications ?
            #supply/@moodCode="EVN" represents medications are dispensed to the patient.
    	    'element':
     		  ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section/'
    		   'hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.1" or @root="2.16.840.1.113883.10.20.22.2.1.1"]/../'

                   'hl7:entry/hl7:substanceAdministration[@moodCode="INT" or @moodCode="EVN"]/' 

                   'hl7:entryRelationship/hl7:supply[@moodCode="EVN"]/'
                   'hl7:statusCode[@code="active" or @code="completed"]/../'
                   'hl7:templateId[@root="2.16.840.1.113883.10.20.22.4.18"]/..'
              )
           
        },

    	'drug_exposure_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not (@nullFlavor="UNK")]',
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
            'fields' : ['person_id', 'visit_occurrence_id', 'drug_concept_id', 'drug_exposure_time',
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
    	'drug_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:product/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:code" ,
    	    'attribute': "code"
    	},
    	'drug_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:product/hl7:manufacturedProduct/hl7:manufacturedMaterial/hl7:code",
    	    'attribute': "codeSystem"
    	},
    	'drug_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,  
    	    'argument_names': {
    		    'concept_code': 'drug_concept_code',
    		    'vocabulary_oid': 'drug_concept_codeSystem',
                'default': 0
    	    },
            'order': 3
    	},

    	'drug_concept_domain_id': {
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'drug_concept_code',
    		    'vocabulary_oid': 'drug_concept_codeSystem',
                'default': 'n/a'
    	    }
    	},
        'drug_exposure_start_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'order': 4
    	},
       'drug_exposure_start_datetime': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'order': 5
    	},
        
        # Drug exposure end date: CCDA Medication Dispense does not provide 
        # an explicit end date (e.g., effectiveTime/high) or sufficient data 
        # (e.g., days_supply, daily dose) to calculate it.
        # Since the end date is required, the end date might be set equal to the start date.
        'drug_exposure_end_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'order': 6
    	},
        'drug_exposure_end_datetime': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'order': 7
    	},
        'verbatim_end_date': {
    	    'config_type': 'PRIORITY',
            'order': 8
        },
        'verbatim_end_date_value': {
    	    'config_type': 'FIELD',
            'data_type': 'DATE',
    	    'element': "hl7:effectiveTime[not(@nullFlavor=\"UNK\")]",
    	    'attribute': "value",
            'priority': ('verbatim_end_date', 1)
    	},
        'verbatim_end_date_high': {
    	    'config_type': 'FIELD',
            'data_type': 'DATE',
            'element': "hl7:effectiveTime/hl7:high[not(@nullFlavor=\"UNK\")]",
    	    'attribute': "value",
            'priority': ('verbatim_end_date', 2)
    	},

        'drug_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32825), # OMOP concept ID for 'EHR dispensing record'
            'order': 9
        },
        
        'stop_reason': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order':10
        },
        'refills': { 'config_type': None, 'order': 11},

        # This approach applies primarily to pre-coordinated consumables, 
        # such as clinical drugs with fixed-dose forms (e.g., tablets, capsules).
        # Example: If the "metoprolol 25mg tablet" is dispensed as 30 tablets,
        #  the extracted quantity will be 30.
        # Needs additional approaches to handle liquid formulations, clinical 
        # drugs with divisible dose forms (e.g., injections), and other quantified 
        # clinical drugs
        'quantity': {
            'config_type': 'FIELD',
            'element': "hl7:quantity",
            'attribute': "value",
            'data_type': 'FLOAT',
            'order': 12
        },
        
        'days_supply': { 'config_type': None, 'order': 13 },
        'sig': { 'config_type': None, 'order': 14 },
        'route_concept_id': { 'config_type': None, 'order': 15 },
        'lot_number': { 'config_type': None, 'order': 16},
                
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
        'drug_source_value': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,  
    	    'argument_names': {
    		    'first_field': 'drug_concept_code',
    		    'second_field': 'drug_concept_codeSystem',
                'default': 'error'
    	    },
            'order': 20
        },

        'drug_source_concept_id': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_source_concept_id,  
            'argument_names': {
                'concept_code': 'drug_concept_code',
                'vocabulary_oid': 'drug_concept_codeSystem',
                'default': 0
            },
            'order': 21 },
        
        'route_source_value': { 'config_type': None, 'order': 22 },

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
