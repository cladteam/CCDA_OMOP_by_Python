
import prototype_2.value_transformations as VT
"""
    This is for caresites from encompassingEncounter/.../healthCareFacility
    Note: TODO need to snoop to check assumptions. So far it looks like we get either and id or a location. Will hash all together to form an id.
    Note: TODO sometimes there are two streetAddressLine attributes. The first is the name. 
       The second is the first line of the address. Ex. 170.314b2_AmbulatoryToC.xml
       I'm calling that an unaccepted violation. The spec. says 0..1.
    Note: TODO sometimes all  you get is an ID.
    Note: TODO are the IDs root-only? or do we sometimes get an extension? 
    Note: TODO sometimes just an address. Ex. 170.314b2_AmbulatoryToC.xml
    NOTE: TODO: consdier the serviceProviderOrganization right next to location in healthCareFacility

    HealthCareFacility: https://build.fhir.org/ig/HL7/CDA-core-sd/StructureDefinition-HealthCareFacility.html
"""
metadata = {
        'Care_Site_ee': {

        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Care_Site',
            'element': "./hl7:componentOf/hl7:encompassingEncounter/hl7:location/hl7:healthCareFacility"
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

        'care_site_name': {
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

        'address_1': {
            'config_type': 'FIELD',
            'element': 'hl7:location/hl7:addr/hl7:streetAddressLine',
            'attribute': "#text",
        },
        # 'address_2'
        'city': {
            'config_type': 'FIELD',
            'element': 'hl7:location/hl7:addr/hl7:city',
            'attribute': "#text",
        },
        'state': {
            'config_type': 'FIELD',
            'element': 'hl7:location/hl7:addr/hl7:state',
            'attribute': "#text",
        },
        'zip': {
            'config_type': 'FIELD',
            'element': 'hl7:location/hl7:addr/hl7:postalCode',
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
