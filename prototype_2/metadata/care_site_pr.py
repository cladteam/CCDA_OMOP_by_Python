
import prototype_2.value_transformations as VT
"""
    This is for caresites from Encounter/participant/participantRole


"""
metadata = {
    'Care_Site_pr': {

        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Care_Site',
            # Encounters sections
            'element': ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section/'
                        'hl7:templateId[ @root="2.16.840.1.113883.10.20.22.2.22" or @root="2.16.840.1.113883.10.20.22.2.22.1" ]/../'
                        'hl7:entry/hl7:encounter[@moodCode="EVN"]/hl7:participant/'
                        'hl7:participantRole[@classCode="SDLOC"]' )
            
        },

        'care_site_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id',
            #    'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "root",
        },
        'care_site_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id',
            #    'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "extension",
        },
        'care_site_id': { 
            'config_type': 'HASH',
            'fields': [ 'care_site_id_root', 'care_site_id_extension'],
            'order': 1
        },
            
        'care_site_name': {  # TBD
            'config_type': 'FIELD',
            'element': 'hl7:playingEntity[@classCode="PLC"]/hl7:name',
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
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
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
        'care_site_source_value': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names':{
                'first_field': 'care_site_id_root',
                'second_field': 'care_site_id_extension',
                'default' : 'error'
            },
            'order': 5
        },
        'place_of_service_source_value': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names':{
                'first_field': 'place_of_service_concept_code',
                'second_field': 'place_of_service_concept_codeSystem',
                'default' : 'error'
            },
            'order': 6
        },

        # addresses here for use in location_id hash above
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
        },
        #'county': {
        #'location_source_value': { TODO: concatentation of fields f"{address_1}|{address_2}|{city} "

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 

    }
}
