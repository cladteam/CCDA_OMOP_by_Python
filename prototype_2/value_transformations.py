import logging
import pandas as pd
logger = logging.getLogger(__name__)

oid_df = pd.read_csv("prototype_2/config/oid.csv")
concept_df = pd.read_csv("prototype_2/config/concept.csv")


def cast_string_to_float(args_dict):
    sv = 0
    string_value = args_dict['input']
    try:
        sv = float(string_value)
    except ValueError:
        logger.error(f"Value error casting {string_value} as integer")
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


def map_oid(codeSystem):
    """ maps an OID used in CCDA to indicate the vocabulary
	to an OMOP vocabulary_id.
	FIX: needs the data, needs written
    """
    try:
        vocabulary_id = oid_df[oid_df['oid'] == codeSystem].vocabulary_id.iloc[0]
        logger.info(" mapping codeSystem \"{codeSystem}\" to \"{vocabulary_id}\" ")
        return vocabulary_id
    except IndexError as e:
        logger.warning(f"no vocab for \"{codeSystem}\" type:{type(e)}")
        return None


def map_to_omop_concept_id(vocabulary_id, concept_code):
    """ Simply maps vocabulary_id, concept_code to an OMOP concept_id.
	Somewhat redundant in that map_to_standard_omop_concept_id
	is much more useful and likely to be used.
    """
    try:
        concept_id = concept_df[concept_df['vocabulary_id'] == vocabulary_id]\
                               [concept_df['concept_code'] == concept_code].concept_id.iloc[0]
        return concept_id
    except IndexError as e:
        logger.warning(f"no concept for \"{vocabulary_id}\" \"{concept_code}\" type:{type(e)}")
        return None


def map_to_omop_domain_id(vocabulary_id, concept_code):
    """ Simply maps vocabulary_id, concept_code to an OMOP domain_id.
	FIX: needs the data, needs written.
	Somewhat redundant in that map_to_standard_omop_concept_id
	is much more useful and likely to be used.
	Definitely redudant with the concept_id function and duplicates cpu effort,
	but fits neatly into the mapping file and keeps the code simple in some respects.
	The key issue is that these functions return a single value, and to do
	both concept_id and domain_id would make them return a list, and then you
	have to know when its a value or a list...
    """
    try:
        domain_id = concept_df[concept_df['vocabulary_id'] == vocabulary_id]\
                              [concept_df['concept_code'] == concept_code].domain_id.iloc[0]
        return domain_id
    except IndexError as e:
        logger.warning(f"no concept for \"{vocabulary_id}\" \"{concept_code}\" type:{type(e)}")
        return None


def map_to_standard_omop_concept_id(vocabulary_id, concept_code):
    """ Maps vocabulary_id, concept_code to a standard OMOP
	concept_id by joining through concept_relationship.
	FIX: needs the data, needs written
    """
    return 2


def map_hl7_to_omop(vocabulary_oid, concept_code):
    """ This would map an HL7 vocabulary_oid to an OMOP vocabulary_id,
	then map both vocabulary_id and concept_code to an OMOP concept_id
    """
    vocabulary_id = map_oid(vocabulary_oid)
    concept_id = map_to_omop_concept_id(vocabulary_id, concept_code)
    # concept_id = map_to_standard_omop_concept_id(vocabulary_id, args_dict['concept_code'])
    return int(concept_id)


def map_hl7_to_omop_w_dict_args(args_dict):
    """ expects: vocabulary_oid, concept_code
    """
    return map_hl7_to_omop(args_dict['vocabulary_oid'], args_dict['concept_code'])


def map_hl7_to_omop_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
    """
    vocabulary_id = map_oid(args_dict['vocabulary_oid'])
    return map_to_omop_domain_id(vocabulary_id, args_dict['concept_code'])
