
import prototype_2.value_transformations as VT
"""
    This is for caresites from encompassingEncounter/.../healthCareFacility
    Note: TODO need to snoop to check assumptions. So far it looks like we get either and id or a location. Will hash all together to form an id.
    Note: TODO sometimes there are two streetAddressLine attributes. The first is the name. The second is the first line of the address. Remember, sometimes all you get is an ID.
    Note: TODO are the IDs root-only? or do we sometimes get an extension? Re

    HealthCareFacility: https://build.fhir.org/ig/HL7/CDA-core-sd/StructureDefinition-HealthCareFacility.html
"""
metadata = {
    'Care_Site': {

        'root': {
            'config_type': 'ROOT',
            'element': "./hl7:componentOf/encompassingEncounter/location/healthCareFacility"
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
        'healthCareFacility_hash_id': { # TODO
            'config_type': 'HASH',
            'element': 'hl7:streetAddressLine',
            'attribute': "#text",
            'priority' : ['care_site_id', 2]
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
    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	        'argument_names': {
    		        'concept_code': 'place_of_service_concept_code',
    		        'vocabulary_oid': 'place_of_service_codeSystem',
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
            'config_type': 'FIELD',
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
            'element': 'hl7:addr/l7:streetAddressLine',
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