import logging
import pandas as pd
logger = logging.getLogger(__name__)

concept_df = pd.read_csv("../CCDA-tools/map_to_standard.csv")


def cast_string_to_float(args_dict):
    sv = 0
    string_value = args_dict['input']
    try:
        sv = float(string_value)
    except ValueError:
        logger.error(f"Value error casting {string_value} as float")
    return sv


def cast_string_to_int(args_dict):
    sv = 0
    string_value = args_dict['input']
    try:
        sv = int(string_value)
    except ValueError:
        logger.error(f"Value error casting {string_value} as integer")
    return sv


def cast_string_to_concept_id(args_dict):
    # string_value = args_dict['input']
    # immediate qustion is if the string_value is a concept_code, which vocabulary_id do you use?
    # next question is at a larger scope, what is the string?
    return ""


def _map_to_omop_concept_row(vocabulary_oid, concept_code):
    """
    """
    try:
        concept_id_df = concept_df[(concept_df['oid'] == vocabulary_oid) &
                                (concept_df['concept_code'] == concept_code)]

        if len(concept_id_df) < 1:
           logger.error(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" ")
           return None

        if len(concept_id_df) > 1:
           logger.warning(f"more than one  concept for \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

        return concept_id_df
    except IndexError as e:
        logger.warning(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" type:{type(e)}")
        return None


def map_hl7_to_omop_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
    """
    return int(_map_to_omop_concept_row(args_dict['vocabulary_oid'], args_dict['concept_code'])['concept_id'].iloc[0])


def map_hl7_to_omop_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
    """
    return _map_to_omop_concept_row(args_dict['vocabulary_oid'], args_dict['concept_code'])['domain_id'].iloc[0]
