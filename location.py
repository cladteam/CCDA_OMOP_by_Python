
# location.py
#
""" Collects OMOP patient address attributes from CCDA patientRole
    depends:
    CCDA document: CCD


TBD!!!! BUG FIX THIS TODO: this gets the patient's home, not the care-site!!!!!


"""

import sys

from util import id_map
from util.xml_ns import ns


def create():
    """ Creates a dictionary with expected fields for populating
        an OMOP location table
    """
    dest = {'location_id': None, 'address': None, 'city': None,
            'state': None, 'zip': None}
    return dest


def _get_location_parts(tree):
    """ EXTRACT: parses a document for location attributes  """
    child = tree.findall(".")[0]

    addresses = child.findall("./recordTarget/patientRole/addr", ns)
    if len(addresses) > 1:
        print("I don't think there should be more than one address here...")
        sys.exit(1)
    if len(addresses) < 1:
        print("No address here...")
        sys.exit(1)
    addr = addresses[0]

    line = addr.find("streetAddressLine", ns).text
    city = addr.find("city", ns).text
    state = addr.find("state", ns).text
    country = addr.find("country", ns).text
    postal_code = addr.find("postalCode", ns).text

    return (line, city, state, country, postal_code)


def get_location_id(tree):
    """ TRANSFORM: parses a document for location attributes
        and fetches and id from the id_map for it
    """

    location_key = _get_location_parts(tree)
    location_id = id_map.get(location_key)
    return location_id


def convert(tree):
    """ ETL: Extracts a row for an OMOP location table from  a top-level
        XML document tree
    """

    (line, city, state, country, postal_code) = _get_location_parts(tree)
    new_id = id_map.get((line, city, state, country, postal_code))

    # LOAD
    dest = create()

    dest['location_id'] = new_id
    dest['address'] = line
    dest['city'] = city
    dest['state'] = state
    dest['zip'] = postal_code

    return dest
