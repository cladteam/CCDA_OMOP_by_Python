
import prototype_2.value_transformations as VT

import prototype_2.metadata.person      as person
import prototype_2.metadata.visit       as visit
import prototype_2.metadata.measurement as measurement
import prototype_2.metadata.observation as observation

""" The meatadata is 3 nested dictionaries:
    - meta_dict: the dict of all domains
    - domain_dict: a dict describing a particular domain
    - field_dict: a dict describing a field component of a domain
    These names are used in the code to help orient the reader

    An output_dict is created for each domain. The keys are the field names,
    and the values are the values of the attributes from the elements.

    FIX: the document as a whole has a few template IDs:
	root="2.16.840.1.113883.10.20.22.1.1"
	root="2.16.840.1.113883.10.20.22.1.2"
"""
meta_dict =  person.metadata | visit.metadata  | measurement.metadata | observation.metadata


def get_meta_dict():
    return meta_dict
