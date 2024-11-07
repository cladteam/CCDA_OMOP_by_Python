import logging
import pandas as pd
from . import concept_df
from . import codemap_xwalk
from . import ccda_value_set_mapping_table_dataset

logger = logging.getLogger(__name__)


def cast_as_string(args_dict):
    string_value = args_dict['input']
    type_value = args_dict['type']
    if type_value == 'ST':
        return str(string_value)
    else:
        return None


def cast_as_number(args_dict):
    string_value = args_dict['input']
    type_value = args_dict['type']
    if type_value == 'PQ':
        return int(string_value)
    else:
        return None


def cast_as_concept_id(args_dict):  # TBD FIX CHRIS
    string_value = args_dict['input']
    type_value = args_dict['type']
    if type_value == 'CD' or type_value == 'CE':
        return string_value
    else:
        return None

    return ""


def _map_to_omop_concept_row(vocabulary_oid, concept_code, default, column_name):
    """
    """
    try:
        concept_id_df = concept_df[(concept_df['oid'] == vocabulary_oid) &
                                (concept_df['concept_code'] == concept_code)]

        if len(concept_id_df) < 1:
           logger.error(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" ")
           return default

        if len(concept_id_df) > 1:
           logger.warning(f"more than one  concept for \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

        if concept_id_df is None:
            return default

        return concept_id_df[column_name].iloc[0]
    except IndexError as e:
        logger.warning(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" type:{type(e)}")
        return default


def map_hl7_to_omop_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
    """
    id_value = _map_to_omop_concept_row(args_dict['vocabulary_oid'],
                                        args_dict['concept_code'],
                                        args_dict['default'],
                                        'concept_id')
    if id_value is not None:
        return int(id_value)
    else:
        return None


def map_hl7_to_omop_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
    """
    return _map_to_omop_concept_row(args_dict['vocabulary_oid'],
                                    args_dict['concept_code'],
                                    args_dict['default'],
                                    'domain_id')


def codemap_xwalk_concept_id(args_dict):
        return _codemap_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_concept_id', args_dict['default']) 

def codemap_xwalk_domain_id(args_dict):
        return _codemap_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_domain_id', args_dict['default']) 

def codemap_xwalk_source_concept_id(args_dict):
        return _codemap_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'source_concept_id', args_dict['default']) 

def _codemap_xwalk(vocabulary_oid, concept_code, column_name, default):
    """ expects: vocabulary_oid, concept_code
    """
    try:
        df = codemap_xwalk[ (codemap_xwalk['vocab_oid'] == vocabulary_oid) &
                            (codemap_xwalk['src_code']  == concept_code) ]
        if len(df) < 1:
           logger.error(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" ")
           return default

        if len(df) > 1:
           logger.warning(f"more than one  concept for \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

        if df is None:
            return default
        return df[column_name].iloc[0]
    except IndexError as e:
        logger.warning(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" type:{type(e)}")
        return default


def valueset_xwalk_concept_id(args_dict):
        return _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_concept_id', args_dict['default']) 

def valueset_xwalk_domain_id(args_dict):
        return _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_domain_id', args_dict['default']) 

def valueset_xwalk_source_concept_id(args_dict):
        return _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'source_concept_id', args_dict['default']) 

def _valueset_xwalk(vocabulary_oid, concept_code, column_name, default):
    """ expects: vocabulary_oid, concept_code
    """
    try:
        df = ccda_value_set_mapping_table_dataset[ (ccda_value_set_mapping_table_dataset['codeSystem'] == vocabulary_oid) &
                                                   (ccda_value_set_mapping_table_dataset['src_cd']  == concept_code) ]
        if len(df) < 1:
           logger.error(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" ")
           return default

        if len(df) > 1:
           logger.warning(f"more than one  concept for \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

        if df is None:
            return default
        return df[column_name].iloc[0]
    except IndexError as e:
        logger.warning(f"no concept for \"{vocabulary_oid}\" \"{concept_code}\" type:{type(e)}")
        return default
    except KeyError as e:
        logger.warning(f"requested field not available \"{vocabulary_oid}\" \"{concept_code}\" type:{type(e)}")
        return default



def map_valuesets_to_omop(args_dicdt):
    """ expects: vocabulary_oid, concept_code
    """
    vocab_oid = args_dict['vocabulary_oid']
    concept_code = args_dict['concept_code']
    codemap_xwalk


def extract_day_of_birth(args_dict):
    # assumes input is ISO-8601 "YYYY-MM-DD"
    date_string = args_dict['date_string']
    if date_string is not None:
        return date_string[8:10]
    return None


def extract_month_of_birth(args_dict):
    # assumes input is ISO-8601 "YYYY-MM-DD"
    date_string = args_dict['date_string']
    if date_string is not None:
        return date_string[5:7]
    return None


def extract_year_of_birth(args_dict):
    # assumes input is ISO-8601 "YYYY-MM-DD"
    date_string = args_dict['date_string']
    if date_string is not None:
        return date_string[0:4]
    return None

