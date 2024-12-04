
import prototype_2.value_transformations as VT
"""
    This is for caresites from Encounter/participant/participantRole


"""
metadata = {
        'Provider': {

        'root': {
            'config_type': 'ROOT',
           
            'element': ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section/hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.10"]/../'
                        'hl7:entry/hl7:encounter/hl7:performer/hl7:assignedEntity' )
        },

        'provider_id': {
            'config_type': 'FIELD',
            'order': 1 ,
            'element': 'hl7:id[@root="2.16.840.1.113883.4.6"]',
            'attribute': "extension",
        },#<code code="207QA0505X" displayName="Adult Medicine Physician" codeSystem="2.16.840.1.113883.6.101" codeSystemName="NUCC" />
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
            'order': 3
        },
    

        #'location_id': {
        #    'config_type': 'HASH',
       #     'fields' : [ 'address_1', 'city', 'state', 'zip'  ],
        #    'order': 4
       # },
     }
}