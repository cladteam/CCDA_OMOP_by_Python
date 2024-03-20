
# location.py
#
""" Collects OMOP patient address attributes from CCDA patientRole
    depends:
    CCDA document: CCD
"""

import sys

import id_map


def create():
    """ Creates a dictionary with expected fields for populating an OMOP location table """
    dest = {'location_id': None, 'address': None, 'city': None,
            'state': None, 'zip': None}
    return dest


def _get_location_parts(tree):
    """ parses a document for location attributes  """
    child_list = tree.findall(".")
    child = child_list[0]

    addresses = child.findall("./{urn:hl7-org:v3}recordTarget/" +
                              "{urn:hl7-org:v3}patientRole/" +
                              "{urn:hl7-org:v3}addr")
    if len(addresses) > 1:
        print("I don't think there should be more than one address here...")
        sys.exit(1)

    addr = addresses[0]
    line = addr.find("{urn:hl7-org:v3}streetAddressLine").text
    city = addr.find("{urn:hl7-org:v3}city").text
    state = addr.find("{urn:hl7-org:v3}state").text
    country = addr.find("{urn:hl7-org:v3}country").text
    postal_code = addr.find("{urn:hl7-org:v3}postalCode").text

    return (line, city, state, country, postal_code)


def get_location_id(tree):
    """ parses a document for location attributes and fetches and id from the id_map for it """

    location_key = _get_location_parts(tree)
    location_id = id_map.get(location_key)
    return location_id


def convert(tree):
    """ Extracts a row for an OMOP location table from  a top-level XML document tree """
    child_list = tree.findall(".")
    child = child_list[0]

    addresses = child.findall("./{urn:hl7-org:v3}recordTarget/" +
                              "{urn:hl7-org:v3}patientRole/" +
                              "{urn:hl7-org:v3}addr")
    if len(addresses) > 1:
        print("I don't think there should be more than one address here...")
        sys.exit(1)

    (line, city, state, country, postal_code) = _get_location_parts(tree)
    location_key = (line, city, state, country, postal_code)
    new_id = id_map.get(location_key)

    dest = create()

    dest['location_id'] = new_id
    dest['address'] = line
    dest['city'] = city
    dest['state'] = state
    dest['zip'] = postal_code

    return dest
