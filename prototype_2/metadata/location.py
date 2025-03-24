
import prototype_2.value_transformations as VT

metadata = {
    'Location': {

        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Location',
            #  header
            'element': "./hl7:recordTarget/hl7:patientRole/hl7:addr"
        },

        'location_id': { 
            'config_type': 'HASH',
            'fields' : [ 'address_1', 'city', 'state', 'zip'  ],
            'order': 1
        },
        'address_1': {
            'config_type': 'FIELD',
            'element': 'hl7:streetAddressLine',
            'attribute': "#text",
            'order': 2
        },
        'address_2': {
            'config_type': None,
            'order': 3
        },
        'city': {
            'config_type': 'FIELD',
            'element': 'hl7:city',
            'attribute': "#text",
            'order': 4
        },
        'state': {
            'config_type': 'FIELD',
            'element': 'hl7:state',
            'attribute': "#text",
            'order': 5
        },
        'zip': {
            'config_type': 'FIELD',
            'element': 'hl7:postalCode',
            'attribute': "#text",
            'order': 6
        },
        'county': {
            'config_type': None,
            'order': 7
        },   
        'location_source_value': { 
            'config_type': None,
            'order': 8
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 

    }
}
