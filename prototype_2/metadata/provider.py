
import prototype_2.value_transformations as VT
"""
    (not) This is for caresites from Encounter/participant/participantRole
    This is for caresites from Encounter/performer/assignedEntity

"""
metadata = {
    'Provider': {

        'root': {
            'config_type': 'ROOT',
           
            'element': ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section/hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.10"]/../'
                        'hl7:entry/hl7:encounter/hl7:performer/hl7:assignedEntity' )
        },
        #   <id extension="444222222" root="2.16.840.1.113883.4.1"/>
        'provider_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id',
            'attribute': "extension",
        },
       'provider_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id',
            'attribute': "root",
        },
        'provider_id': {
            'config_type': 'HASH',
            'fields' : [ 'provider_id_extension', 'provider_id_root'],
            'order': 1 ,
        },

        'provider_name': { 
            'config_type': 'None',
            'order': 2
        },

        'npi': {
            'config_type': 'FIELD',
            'element': 'hl7:id[@root="2.16.840.1.113883.4.6"]',
            'attribute': "extension",
            'order': 3,
        },
        'dea': {
            'config_type': 'FIELD',
            'element': 'hl7:id[@root="2.16.840.1.113883.D.E.A"]', # TODO get the correct OID
            'attribute': "extension",
            'order': 4,
        },
        #<code code="207QA0505X" displayName="Adult Medicine Physician" codeSystem="2.16.840.1.113883.6.101" codeSystemName="NUCC" />
        'specialty_id_code': { 
            'config_type': 'FIELD',
            'element': 'hl7:code',
            'attribute': "code",
        },
        'specialty_id_codeSystem': { 
            'config_type': 'FIELD',
            'element': 'hl7:code',
            'attribute': "codesystem",
        },
        'specialty_id': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	        'argument_names': {
    		        'concept_code': 'specialty_id_code',
    		        'vocabulary_oid': 'specialty_id_codeSystem',
                    'default': 0
    	         },
            'order': 5
        },
    

        #'location_id': {
        #    'config_type': 'HASH',
       #     'fields' : [ 'address_1', 'city', 'state', 'zip'  ],
        #    'order': 4
       # },
     }
}