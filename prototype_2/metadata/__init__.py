
import prototype_2.value_transformations as VT

import prototype_2.metadata.person      as person
import prototype_2.metadata.visit       as visit
import prototype_2.metadata.measurement as measurement
import prototype_2.metadata.measurement_vital_signs as measurement_vs
import prototype_2.metadata.observation as observation
import prototype_2.metadata.location as location
import prototype_2.metadata.care_site_ee_hcf_location as care_site_ee_hcf_location
import prototype_2.metadata.care_site_ee_hcf as care_site_ee_hcf
import prototype_2.metadata.care_site_pr_location as care_site_pr_location
import prototype_2.metadata.care_site_pr as care_site_pr
import prototype_2.metadata.provider as provider
import prototype_2.metadata.provider_header_documentationOf as provider_header_documentationOf
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
#  NB: Order is important here.
#  PKs like person and visit must come before referencing FK configs, like in measurement

meta_dict =  location.metadata | \
             provider_header_documentationOf.metadata | \
             person.metadata | \
             visit.metadata  | \
             measurement.metadata | \
             measurement_vs.metadata | \
             observation.metadata  | \
             care_site_ee_hcf.metadata | \
             care_site_ee_hcf_location.metadata | \
             care_site_pr.metadata | \
             care_site_pr_location.metadata | \
             provider.metadata



def get_meta_dict():
    return meta_dict
