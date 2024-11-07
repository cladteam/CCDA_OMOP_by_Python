
import prototype_2.value_transformations as VT

metadata = {
    'Location': {

        'root': {
            'config_type': 'ROOT',
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
        # 'address_2'
        'city': {
            'config_type': 'FIELD',
            'element': 'hl7:city',
            'attribute': "#text",
            'order': 3
        },
        'state': {
            'config_type': 'FIELD',
            'element': 'hl7:state',
            'attribute': "#text",
            'order': 4
        },
        'zip': {
            'config_type': 'FIELD',
            'element': 'hl7:postalCode',
            'attribute': "#text",
            'order': 5
        }
        #'county': {
        #'location_source_value': { TODO: concatentation of fields f"{address_1}|{address_2}|{city} "

    }
}