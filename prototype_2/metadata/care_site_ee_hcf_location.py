
import prototype_2.value_transformations as VT
"""
    This is for caresites from encompassingEncounter/.../healthCareFacility
"""
metadata = {
    'Location': {

        'root': {
            'config_type': 'ROOT',
            'element': "./hl7:componentOf/encompassingEncounter/location/healthCareFacility/location"
        },
        # TODO do we care about the use="WP" attribute?
        'location_id': {
            'config_type': 'HASH',
            'fields' : [ 'address_1', 'city', 'state', 'zip'  ],
            'order': 1
        },
        'address_1': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/l7:streetAddressLine',
            'attribute': "#text",
            'order': 2
        },
        'address_2' : { 'config_type': None, 'order': 3 },
        'city': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:city',
            'attribute': "#text",
            'order': 4
        },
        'state': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:state',
            'attribute': "#text",
            'order': 5
        },
        'zip': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:postalCode',
            'attribute': "#text",
            'order': 6
        },
        'county': {'config_type': None, 'order': 7 },
        'location_source_value':  {'config_type': None, 'order': 8 }
                #TODO: concatentation of fields f"{address_1}|{address_2}|{city} "

    }
}