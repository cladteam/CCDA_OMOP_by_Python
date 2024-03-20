
# observation.py
"""
     collects OMOP patient observation (TBD) attributes from CCDA patientRole
     depends: patient.py must be run on this document beforehand so the ID has been entered.
     CCDA document: CCD

    ToDo:
    - person_id
    - provider_id
    - value type
"""

import vocab_map_file
import util
import person


def create():
    """ Creates a dictionary with expected fields for populating an OMOP observation table """
    dest = {'observation_id': None, 'person_id': None, 'observation_concept_id': None,
            'observation_date': None,
            'value_as_number': None, 'value_as_string': None, 'value_as_concept_id': None,
            'provider_id': None}
    return dest


def convert(tree):
    """ Extracts a row for an OMOP observation table from  a top-level XML document tree """
    child = tree.findall(".")[0]

    person_id = person.get_person_id(tree)

    # documentationOf = child.findall("./{urn:hl7-org:v3}documentationOf") # clinician, Dx
    # componentOf = child.findall("./{urn:hl7-org:v3}componentOf") # encompassing encounter, visit

    results_section = child.findall("./{urn:hl7-org:v3}component/" +
                                    "{urn:hl7-org:v3}structuredBody/" +
                                    "{urn:hl7-org:v3}component/" +
                                    "{urn:hl7-org:v3}section/" +
                                    "{urn:hl7-org:v3}templateId[@root='" +
                                    vocab_map_file.RESULTS + "']/..")

    results_observations = results_section[0].findall("{urn:hl7-org:v3}entry/" +
                                                      "{urn:hl7-org:v3}organizer/" +
                                                      "{urn:hl7-org:v3}component/" +
                                                      "{urn:hl7-org:v3}observation")

    dest = list(range(len(results_observations)))
    i = 0
    for obs in results_observations:
        observation_id = obs.find("{urn:hl7-org:v3}id").attrib['root']
        # observation_id = obs.find("{urn:hl7-org:v3}id").attrib['extension']

        observation_code = obs.find("{urn:hl7-org:v3}code")
        observation_concept_id = vocab_map_file.map_hl7_to_omop(
            observation_code.attrib['codeSystem'], observation_code.attrib['code'])

        observation_date_string = obs.find("{urn:hl7-org:v3}effectiveTime").attrib['value']
        observation_date = util.convert_date(observation_date_string)

        observation_value = obs.find("{urn:hl7-org:v3}value")
        observation_value_value = observation_value.attrib['value']
        # observation_value_type = observation_value.attrib['xsi:type']
        observation_value_type = observation_value.\
            attrib['{http://www.w3.org/2001/XMLSchema-instance}type']
        # observation_value_unit = observation_value.attrib['unit']

        dest[i] = create()
        dest[i]['observation_id'] = observation_id
        dest[i]['person_id'] = person_id
        dest[i]['observation_concept_id'] = observation_concept_id
        dest[i]['observation_date'] = observation_date
        # observation value TYPE???  #############
        if observation_value_type == 'PQ':
            dest[i]['value_as_number'] = observation_value_value
        elif observation_value_type == 'ST':
            dest[i]['value_as_number'] = None
            dest[i]['value_as_string'] = observation_value
        dest[i]['value_as_concept_id'] = None
        dest[i]['provider_id'] = None  # ###############
        i += 1

    return dest

    # child {urn:hl7-org:v3}templateId
    # child {urn:hl7-org:v3}referenceRange (low, high)
    # child {urn:hl7-org:v3}text
    # child {urn:hl7-org:v3}statusCode
    # child {urn:hl7-org:v3}interpretationCode
    # child {urn:hl7-org:v3}methodCode
    # child {urn:hl7-org:v3}targetSiteCode
    # child {urn:hl7-org:v3}author
