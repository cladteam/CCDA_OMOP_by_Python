
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

from util import vocab_map_file
from util import vocab_spark
from util.xml_ns import ns
from util import util
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
    child = tree.findall(".", ns)[0]

    person_id = person.get_person_id(tree)

    # documentationOf = child.findall("./documentationOf", ns) # clinician, Dx
    # componentOf = child.findall("./componentOf", ns) # encompassing encounter, visit

    results_section = child.findall("./component/structuredBody/component/section/" +
                                    "templateId[@root='" + vocab_map_file.RESULTS + "']/..", ns)

    results_observations = results_section[0].findall("entry/organizer/component/observation", ns)

    dest = list(range(len(results_observations)))
    i = 0
    for obs in results_observations:
        try:
            template_id = obs.find("templateId", ns).attrib['root']
            observation_id = obs.find("id", ns).attrib['root']
            if template_id != '2.16.840.1.113883.10.20.22.4.2':
                print(f"INFO: observation template:{template_id} id:{observation_id}")
            # observation_id = obs.find("id", ns).attrib['extension']

            observation_code = obs.find("code", ns)
            # observation_concept_id = vocab_map_file.map_hl7_to_omop(
            #    observation_code.attrib['codeSystem'], observation_code.attrib['code'])
            observation_concept_id = vocab_spark.map_hl7_to_omop(
                observation_code.attrib['codeSystem'], observation_code.attrib['code'])

            observation_date_string = obs.find("effectiveTime", ns).attrib['value']
            observation_date = util.convert_date(observation_date_string)

            observation_value = obs.find("value", ns)
            observation_value_value = observation_value.attrib['value']
            observation_value_type = observation_value.attrib['{' + ns['xsi'] + '}type']
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
        except KeyError as kex:
            print(f"""WARN: KeyError on observation with id {observation_id}
                and tempalteId {template_id}, skipped missing key:""", kex)

    return dest

    # child templateId
    # child referenceRange (low, high)
    # child text
    # child statusCode
    # child interpretationCode
    # child methodCode
    # child targetSiteCode
    # child author
