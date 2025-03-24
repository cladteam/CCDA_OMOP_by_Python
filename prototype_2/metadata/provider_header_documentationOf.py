
import prototype_2.value_transformations as VT
"""
    This is for providers from the header.

"""
metadata = {
    'Provider_header_documentationOf': {

        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Provider',
            # from the header 
            'element': ('./hl7:documentationOf/hl7:serviceEvent/hl7:performer[@typeCode="PRF"]/hl7:assignedEntity' )
        },
      
        'provider_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "extension"
        },
       'provider_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "root"
        },
        'provider_id_element': {
            'config_type': 'HASH',
            'fields': [ 'provider_id_root', 'provider_id_extension'],
            'priority' : ['provider_id', 1]
        },
        
        'provider_id_street': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:streetAddressLine',
            'attribute': "#text"
        },
       'provider_id_city': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:city',
            'attribute': "#text"
        },
        'provider_id_state': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:state',
            'attribute': "#text"
        },
        'provider_id_zip': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:postalCode',
            'attribute': "#text"
        },
        'provider_id_given': {
            'config_type': 'FIELD',
            'element': 'hl7:assignedPerson/hl7:name/hl7:given',
            'attribute': "#text"
        },
        'provider_id_family': {
            'config_type': 'FIELD',
            'element': 'hl7:assignedPerson/hl7:name/hl7:family',
            'attribute': "#text"
        },
        'provider_id_hash': {
            'config_type': 'HASH',
            'fields' : [ 'provider_id_street', 'provider_id_city', 'provider_id_state', 'provider_id_zip', 'provider_id_given', 'provider_id_family'],
            'priority' : ['provider_id', 2]
        },
       
        'provider_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },

        'provider_name': { 
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names':{
                'first_field': 'provider_id_given',
                'second_field': 'provider_id_family',
                'default' : 'n/a'
            },
            'order': 2
        },

        'npi': {
            'config_type': 'FIELD',
            'element': 'hl7:id[@root="2.16.840.1.113883.4.6"]',
            'attribute': "extension",
            'order': 3
        },
        'dea': {
            'config_type': 'FIELD',
            'element': 'hl7:id[@root="2.16.840.1.113883.D.E.A"]', # TODO get the correct OID
            'attribute': "extension",
            'order': 4
        },
        #<code code="207QA0505X" displayName="Adult Medicine Physician" codeSystem="2.16.840.1.113883.6.101" codeSystemName="NUCC" />
        'specialty_concept_id_code': { 
            'config_type': 'FIELD',
            'element': 'hl7:code',
            'attribute': "code"
        },
        'specialty_concept_id_codeSystem': { 
            'config_type': 'FIELD',
            'element': 'hl7:code',
            'attribute': "codesystem"
        },
        'specialty_concept_id': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	        'argument_names': {
    		        'concept_code': 'specialty_concept_id_code',
    		        'vocabulary_oid': 'specialty_concept_id_codeSystem',
                    'default': 0
    	         },
            'order': 5
        },
        
        # hl7:encounter/hl7:participant/hl7:participantRole
        'care_site_id_root': { 
            'config_type': 'FIELD',
            'element': '../../hl7:participant/hl7:participantRole/hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "root",
        },
        'care_site_id_extension': { 
            'config_type': 'FIELD',
            'element': '../../hl7:participant/hl7:participantRole/hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "extension",
        },
        'care_site_id': { 
            'config_type': 'HASH',
            'fields': [ 'care_site_id_root', 'care_site_id_extension'],
            'order': 6
        },
  
        'year_of_birth': {
            'config_type': None,
            'order' :7
        },
        'gender_concept_id': {
            'config_type': None,
            'order' :8
        },
        'provider_source_value': {
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':9
        },
        'specialty_source_value': {
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':10
        },
        'specialty_source_concept_id': {
            'config_type': None,
            'order' :11
        }, 
        'gender_source_value': {
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':12
        },
        'gender_source_concept_id': {
            'config_type': None,
            'order' :13
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 
    },
    
}
