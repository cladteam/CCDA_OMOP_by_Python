
# person.py
""" collects OMOP patient attributes from CCDA patientRole
    depends: location.py must be run on this document beforehand so the ID has been entered.
    CCDA document: CCD

    ToDo: is a template_id associated with this section? Does it change from document to document?
"""

#import vocab_map_file
import vocab_spark
from xml_ns import ns
import util
import location
import omop_person


def create():
    """ Creates a dictionary with expected fields for populating an OMOP person table """
    dest = {'person_id': None, 'race_concept_id': None, 'ethnicity_concept_id': None,
            'gender_concept_id': None, 'birthdate': None, 'location_id': None}
    return dest


def get_person_id(tree):
    """ EXTRACT/TRANSFORM:finds the patientRole id from a CCDA document, for use in OMOP person references """
    ssn_root = "2.16.840.1.113883.4.1"
    # HL7_root = "2.16.840.1.113883.4.6"

    child = tree.findall(".", ns)[0]
    person_id_list = child.findall("./recordTarget/patientRole/id[@root='" + ssn_root + "']", ns)
    person_id = person_id_list[0].attrib['extension']

    return person_id


def convert(tree):
    """ Extracts a row for an OMOP person table from  a top-level XML document tree """
    child = tree.findall(".", ns)[0]

    # GET LOCATION KEY
    location_id = location.get_location_id(tree)

    # GET PATIENT ATTRIBUTES
    patient = child.findall("./recordTarget/patientRole/patient", ns)[0]

    race_code = patient.find("raceCode", ns)
    # race_concept_id = vocab_map_file.map_hl7_to_omop(
    race_concept_id = vocab_spark.map_hl7_to_omop(
        race_code.get("codeSystem"), race_code.get("code"))
    if race_concept_id is None:
        print(f"None concept from {race_code.get('codeSystem')}, {race_code.get('code')}")

    ethnicity_code = patient.find("ethnicGroupCode", ns)
    # ethnicity_concept_id = vocab_map_file.map_hl7_to_omop(
    ethnicity_concept_id = vocab_spark.map_hl7_to_omop(
        ethnicity_code.get("codeSystem"), ethnicity_code.get("code"))
    if ethnicity_concept_id is None:
        print(f"None concept from {ethnicity_code.get('codeSystem')}, {ethnicity_code.get('code')}")

    gender_code = patient.find("administrativeGenderCode", ns)
    # gender_concept_id = vocab_map_file.map_hl7_to_omop(
    gender_concept_id = vocab_spark.map_hl7_to_omop(
        gender_code.get("codeSystem"), gender_code.get("code"))
    if gender_concept_id is None:
        print(f"No concept from {gender_code.get('codeSystem')}, {gender_code.get('code')}")

    birth_date_string = patient.find("birthTime", ns).get("value")
    birth_date = util.convert_date(birth_date_string)

    # GET PATIENT ID
    person_id = get_person_id(tree)

    # LOAD
    dest = create()

    dest['person_id'] = person_id
    dest['race_concept_id'] = race_concept_id
    dest['ethnicity_concept_id'] = ethnicity_concept_id
    dest['gender_concept_id'] = gender_concept_id
    dest['birthdate'] = birth_date
    dest['location_id'] = location_id

    return dest
 
    # person_obj = omop_person.OmopPerson(person_id, gender_concept_id, birth_date_omop, race_concept_id, ethnicity_concept_id, location_id)
    # return  person_obj.create_dictionary()
