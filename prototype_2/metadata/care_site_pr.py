
import prototype_2.value_transformations as VT
"""
    This is for caresites from Encounter/participant/participantRole


"""
metadata = {
        'Care_Site_pr': {

        'root': {
            'config_type': 'ROOT',
            #'element': ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section/hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.22"]/../'
            #            'hl7:entry/hl7:encounter[@moodCode=”EVN"]/hl7:participant/hl7:participantRole[@classCode="SDLOC"]' )
            'element': ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section/hl7:templateId[@root="2.16.840.1.113883.10.20.22.2.22.1"]/../'
                        'hl7:entry/hl7:encounter/hl7:participant/hl7:participantRole' )
        },

        'care_site_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },
        'healthCareFacility_id': {
            'config_type': 'FIELD',
            'element': 'hl7:id',
            'attribute': "root",
            'priority' : ['care_site_id', 1]
        },
       # 'healthCareFacility_hash_id': { # TODO
       #     'config_type': 'HASH',
       #     'element': 'hl7:streetAddressLine',
       #     'attribute': "#text",
       #     'priority' : ['care_site_id', 2]
       #},

        'care_site_name': {  # TBD
            'config_type': 'FIELD',
            'element': 'hl7:location/hl7:name',
            'attribute': "#text",
            'order': 2
        },

        'place_of_service_concept_code': {
            'config_type': 'FIELD',
            'element': 'hl7:code',
            'attribute': "code",
        },
        'place_of_service_concept_codeSystem': {
            'config_type': 'FIELD',
            'element': 'hl7:code',
            'attribute': "codeSystem",
        },
        'place_of_service_concept_id': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	        'argument_names': {
    		        'concept_code': 'place_of_service_concept_code',
    		        'vocabulary_oid': 'place_of_service_concept_codeSystem',
                    'default': 0
    	         },
            'order': 3
        },

        'location_id': {
            'config_type': 'HASH',
            'fields' : [ 'address_1', 'city', 'state', 'zip'  ],
            'order': 4
        },
        'care_site_source_value': { # TODO concat id and address fileds
            'config_type': 'None',
            'order': 5
        },
        'place_of_service_source_value': { # TODO concat code and codeSystem?
            'config_type': 'FIELD',
            'element': 'hl7:code',
            'attribute': "code",
            'order': 6
        },

        'address_1': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:streetAddressLine',
            'attribute': "#text",
        },
        # 'address_2'
        'city': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:city',
            'attribute': "#text",
        },
        'state': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:state',
            'attribute': "#text",
        },
        'zip': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:postalCode',
            'attribute': "#text",
        }
        #'county': {
        #'location_source_value': { TODO: concatentation of fields f"{address_1}|{address_2}|{city} "

    }
}
