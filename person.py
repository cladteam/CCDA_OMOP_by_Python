
# person.py
""" collects OMOP patient attributes from CCDA patientRole
    depends: location.py must be run on this document beforehand so the ID has been entered.
    CCDA document: CCD

    ToDo: is a template_id associated with this section? Does it change from document to document?
"""

import id_map
from vocab_map_file import vocab_map
import util


def create():
    """ Creates a dictionary with expected fields for populating an OMOP person table """
    dest = {'person_id': None, 'race_concept_id': None, 'ethnicity_concept_id': None,
            'gender_concept_id': None, 'birthdate': None, 'location_id': None}
    return dest


def get_person_id(tree):
    """ finds the patientRole id from a CCDA document, for use in OMOP person references """
    ssn_root = "2.16.840.1.113883.4.1"
    # HL7_root = "2.16.840.1.113883.4.6"

    child_list = tree.findall(".")
    child = child_list[0]
    person_id_list = child.findall("./{urn:hl7-org:v3}recordTarget/" +
                                   "{urn:hl7-org:v3}patientRole/" +
                                   "{urn:hl7-org:v3}id[@root='" + ssn_root + "']")
    person_id = person_id_list[0].attrib['extension']

    return person_id


def convert(tree):
    """ Extracts a row for an OMOP person table from  a top-level XML document tree """
    child_list = tree.findall(".")
    child = child_list[0]

    # GET LOCATION KEY
    addr = child.findall("./{urn:hl7-org:v3}recordTarget/" +
                         "{urn:hl7-org:v3}patientRole/" +
                         "{urn:hl7-org:v3}addr")[0]

    line = addr.find("{urn:hl7-org:v3}streetAddressLine").text
    city = addr.find("{urn:hl7-org:v3}city").text
    state = addr.find("{urn:hl7-org:v3}state").text
    country = addr.find("{urn:hl7-org:v3}country").text
    postal_code = addr.find("{urn:hl7-org:v3}postalCode").text

    location_key = (line, city, state, country, postal_code)
    location_id = id_map.get(location_key)

    # GET PATIENT ATTRIBUTES
    patient = child.findall("./{urn:hl7-org:v3}recordTarget/" +
                            "{urn:hl7-org:v3}patientRole/" +
                            "{urn:hl7-org:v3}patient")[0]

    race_code = patient.find("{urn:hl7-org:v3}raceCode")
    race_vocabulary_id = race_code.get("codeSystem")
    race_concept_code = race_code.get("code")
    (_, race_concept_id, _, _) = vocab_map[(race_vocabulary_id, race_concept_code)]
    if race_concept_id is None:
        print(f"No concept from {(race_vocabulary_id, race_concept_code)}")

    ethnicity_code = patient.find("{urn:hl7-org:v3}ethnicGroupCode")
    ethnicity_vocabulary_id = ethnicity_code.get("codeSystem")
    ethnicity_concept_code = ethnicity_code.get("code")
    (_, ethnicity_concept_id, _, _) = vocab_map[(ethnicity_vocabulary_id, ethnicity_concept_code)]
    if ethnicity_concept_id is None:
        print(f"No concept from {(ethnicity_vocabulary_id, ethnicity_concept_code)}")

    gender_code = patient.find("{urn:hl7-org:v3}administrativeGenderCode")
    gender_vocabulary_id = gender_code.get("codeSystem")
    gender_concept_code = gender_code.get("code")
    (_, gender_concept_id, _, _) = vocab_map[(gender_vocabulary_id, gender_concept_code)]
    if gender_concept_id is None:
        print(f"No concept {gender_concept_id} " +
              "from {(gender_vocabulary_id, gender_concept_code)}")

    birth_date_string = patient.find("{urn:hl7-org:v3}birthTime").get("value")
    birth_date = util.convert_date(birth_date_string)

    # GET PATIENT ID
    person_id = get_person_id(tree)

    dest = create()

    dest['person_id'] = person_id
    dest['race_concept_id'] = race_concept_id
    dest['ethnicity_concept_id'] = ethnicity_concept_id
    dest['gender_concept_id'] = gender_concept_id
    dest['birthdate'] = birth_date
    dest['location_id'] = location_id

    return dest
