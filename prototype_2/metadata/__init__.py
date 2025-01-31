
import prototype_2.value_transformations as VT

import prototype_2.metadata.person      as person
import prototype_2.metadata.visit_encompassingEncounter as visit_encompassingEncounter
import prototype_2.metadata.visit       as visit
import prototype_2.metadata.measurement as measurement
import prototype_2.metadata.measurement_vital_signs as measurement_vs
import prototype_2.metadata.observation as observation
import prototype_2.metadata.condition as condition
import prototype_2.metadata.location as location
import prototype_2.metadata.care_site_ee_hcf_location as care_site_ee_hcf_location
import prototype_2.metadata.care_site_ee_hcf as care_site_ee_hcf
import prototype_2.metadata.care_site_pr_location as care_site_pr_location
import prototype_2.metadata.care_site_pr as care_site_pr
import prototype_2.metadata.provider as provider
from prototype_2.metadata import visit_encompassingEncounter
import prototype_2.metadata.provider_header_documentationOf as provider_header_documentationOf
import prototype_2.metadata.medication_medication_dispense as medication_medication_dispense
import prototype_2.metadata.medication_medication_activity as medication_medication_activity
""" The meatadata is 3 nested dictionaries:
    - meta_dict: the dict of all domains
    - domain_dict: a dict describing a particular domain
    - field_dict: a dict describing a field component of a domain
    These names are used in the code to help orient the reader

    An output_dict is created for each domain. The keys are the field names,
    and the values are the values of the attributes from the elements.

    REMEMBER to update the ddl.py file as well.
"""
#  NB: Order is important here.
#  PKs like person and visit must come before referencing FK configs, like in measurement

meta_dict =  location.metadata | \
             provider_header_documentationOf.metadata | \
             person.metadata | \
             visit_encompassingEncounter.metadata  | \
             visit.metadata  | \
             measurement.metadata | \
             measurement_vs.metadata | \
             observation.metadata  | \
             medication_medication_dispense.metadata | \
             medication_medication_activity.metadata | \
             condition.metadata | \
             care_site_ee_hcf.metadata | \
             care_site_ee_hcf_location.metadata | \
             care_site_pr.metadata | \
             care_site_pr_location.metadata | \
             provider.metadata




def get_meta_dict():
    return meta_dict
