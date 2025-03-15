#!/usr/bin/env python3

""" Table-Driven ElementTree parsing in Python

 This version puts the paths into a data structure and explores using
 one function driven by the data.
 - The mapping_dict is hard-coded here. An next step would be to read that in from a file.
 - Value transformation is stubbed out waiting for vocabularies to be loaded, and to
   figure out how to use them once there.

  - Deterministic hashing in Python3 https://stackoverflow.com/questions/27954892/deterministic-hashing-in-python-3
  - https://stackoverflow.com/questions/16008670/how-to-hash-a-string-into-8-digits 

 Chris Roeder

    Call Graph:
    - main
      - process_file
        - parse_doc
          -  parse_configuration_from_file
            - parse_config_from_single_root
              - do_none_fields
              - do_constant_fields
              - do_basic_fields
              - do_derived_fields
              - do_domain_fields
              - do_hash_fields
              - do_priority_fields


    Config dictionary structure: dict[str, dict[str, dict[str, str ] ] ]
    metadata = {
        config_dict = {
            field_details_dict = {
               attribute: value 
            }
        }
    }
    So there are many config_dicts, each roughly for a domain. You may
    have more than one per domain when there are more than a single
    location for a domain.
    Each config_dict is made up of many fields for the OMOP table it 
    creates. There are non-output fields used as input to derived 
    fields, like the vocabulary and code used to find the concept_id.
    Each field_spec. has multiple attributes driving that field's
    retrieval or derivation.
    
    PK_dict :dict[str, any]
    key is the field_name, any is the value. Value can be a string, int, None or a list of same.
    
    output_dict :dict[str, any]
    omop_dict : dict[str, list[any] for each config you have a list of records
    


    XML terms used specifically:
    - element is a thing in a document inside angle brackets like <code code="1234-5" codeSystem="LOINC"/
    - attributes are code and codeSystem in the above example
    - text is when there are both start and end parts to the element like <text>foobar</text>. "foobar" is
       the text in an element that has a tag = 'text'
    - tag see above

"""

# import pandas as pd
# mamba install -y -q lxml

import argparse
import datetime
from dateutil.parser import parse
import logging
import os
import sys
import hashlib
import zlib
#import ctypes
from numpy import int32
from numpy import int64
import traceback
from collections import defaultdict
from lxml import etree as ET
from lxml.etree import XPathEvalError
from typeguard import typechecked
from prototype_2 import value_transformations as VT

from prototype_2.metadata import get_meta_dict
 

logger = logging.getLogger(__name__)


ns = {
   # '': 'urn:hl7-org:v3',  # default namespace
   'hl7': 'urn:hl7-org:v3',
   'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
   'sdtc': 'urn:hl7-org:sdtc'
}


#from foundry.transforms import Dataset
#concept_xwalk = Dataset.get("concept_xwalk")
#concept_xwalk_files = concept_xwalk.files().download()

#def create_8_byte_hash(input_string):
#    hash_value = hashlib.md5(input_string.encode('utf-8'))
#    int_hash_value = int(hash_value.hexdigest(), 16)
#    bigint_hash_value = ctypes.c_int64(int_hash_value % 2**64).value
#    return bigint_hash_value

@typechecked
def create_hash(input_string) -> int | None:
    """ matches common SQL code when that code also truncates to 13 characters
        SQL: cast(conv(substr(md5(test_string), 1, 15), 16, 10) as bigint) as hashed_value
    """
    if input_string == '':
        return None
    
    hash_value = hashlib.md5(input_string.encode('utf-8').upper())
    truncated_hash = hash_value.hexdigest()[0:13]
    int_trunc_hash_value = int(truncated_hash, 16)
    return int_trunc_hash_value


@typechecked
def cast_to_date(string_value) ->  datetime.date | None:
    # TODO does CCDA always do dates as YYYYMMDD ?
    # https://build.fhir.org/ig/HL7/CDA-ccda/StructureDefinition-USRealmDateTimeInterval-definitions.html
    # doc says YYYMMDD... examples show ISO-8601. Should use a regex and detect parse failure.
    # TODO  when  is it date and when datetime

    try:
        datetime_val = parse(string_value)
        return datetime_val.date()
    except Exception as x:
        print(f"ERROR couldn't parse {string_value} as date. Exception:{x}")
        return None
    except ValueError as ve:
        print(f"ERROR couldn't parse {string_value} as date. ValueError:{ve}")
        return None

def cast_to_datetime(string_value) -> datetime.datetime | None:
    try:
        datetime_val = parse(string_value)
        return datetime_val
    except Exception as x:
        print(f"ERROR couldn't parse {string_value} as datetime. {x}")
        return None


@typechecked
def parse_field_from_dict(field_details_dict :dict[str, str], root_element, 
        config_name, field_tag, root_path) ->  None | str | float | int | int32 | datetime.datetime | datetime.date:
    """ Retrieves a value for the field descrbied in field_details_dict that lies below
        the root_element.
        Domain and field_tag are here for error messages.
    """

    if 'element' not in field_details_dict:
#        logger.error(("FIELD could find key 'element' in the field_details_dict:"
#                     f" {field_details_dict} root:{root_path}"))
        return None

    logger.info(f"    FIELD {field_details_dict['element']} for {config_name}/{field_tag}")
    field_element = None
    try:
        field_element = root_element.xpath(field_details_dict['element'], namespaces=ns)
    except XPathEvalError as p:
        pass
#        logger.error(f"ERROR (often inconsequential) {field_details_dict['element']} {p}")
        ###print(f"FAILED often inconsequential  {field_details_dict['element']} {p}")
    if field_element is None:
##        logger.error((f"FIELD could not find field element {field_details_dict['element']}"
##                      f" for {config_name}/{field_tag} root:{root_path} {field_details_dict} "))
        return None

    if 'attribute' not in field_details_dict:
##        logger.error((f"FIELD could not find key 'attribute' in the field_details_dict:"
##                     f" {field_details_dict} root:{root_path}"))
        return None

    logger.info((f"       ATTRIBUTE   {field_details_dict['attribute']} "
                 f"for {config_name}/{field_tag} {field_details_dict['element']} "))
    attribute_value = None
    if len(field_element) > 0:
        attribute_value = field_element[0].get(field_details_dict['attribute'])
        if field_details_dict['attribute'] == "#text":
            try:
                attribute_value = ''.join(field_element[0].itertext())
            except Exception as e:
                logger.error((f"no text elemeent for field element {field_element} "
                        f"for {config_name}/{field_tag} root:{root_path} "
                        f" dict: {field_element[0].attrib} EXCEPTION:{e}"))
        if attribute_value is None:
            logger.warning((f"no value for field element {field_details_dict['element']} "
                        f"for {config_name}/{field_tag} root:{root_path} "
                        f" dict: {field_element[0].attrib}"))
    else:
        logger.warning((f"no element at path {field_details_dict['element']} "
                        f"for {config_name}/{field_tag} root:{root_path} "))

    # Do data-type conversions
    if 'data_type' in field_details_dict:
        if attribute_value is not None:
            if field_details_dict['data_type'] == 'DATE':
                try:
                    attribute_value = cast_to_date(attribute_value)
                except Exception as e:
                    print(f"cast to date failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to date failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'DATETIME':
                try:
                    attribute_value = cast_to_datetime(attribute_value)
                except Exception as e:
                    print(f"cast to datetime failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to datetime failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'LONG':
                try:
                    attribute_value = int64(attribute_value)
                except Exception as e:
                    print(f"cast to int64 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to int64 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'INTEGER':
                try:
                    attribute_value = int32(attribute_value)
                except Exception as e:
                    print(f"cast to int32 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to int32 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'BIGINTHASH':
                try:
                    attribute_value = create_hash(attribute_value)
                except Exception as e:
                    print(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'TEXT':
                try:
                    attribute_value = str(attribute_value)
                except Exception as e:
                    print(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'FLOAT':
                try:
                    attribute_value = float(attribute_value)
                except Exception as e:
                    print(f"cast to float failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to float failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            else:
                print(f" UNKNOWN DATA TYPE: {field_details_dict['data_type']} {config_name} {field_tag}")
                logger.error(f" UNKNOWN DATA TYPE: {field_details_dict['data_type']} {config_name} {field_tag}")
            return attribute_value
        else:
        #    print(f" no value: {field_details_dict['data_type']} {config_name} {field_tag}")
        #   logger.error(f" no value: {field_details_dict['data_type']} {config_name} {field_tag}")
            return None
    else:
        return attribute_value


@typechecked
def do_none_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date ],
                   root_element, root_path, config_name,  
                   config_dict :dict[str, dict[str, str | None]], 
                   error_fields_set :set[str]):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     NONE FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag is None:
            output_dict[field_tag] = None

            
@typechecked
def do_constant_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date], 
                       root_element, root_path, config_name,  
                       config_dict :dict[str, dict[str, str | None]], 
                       error_fields_set :set[str]):

    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     CONSTANT FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag == 'CONSTANT':
            constant_value = field_details_dict['constant_value']
            output_dict[field_tag] = constant_value

            
@typechecked
def do_filename_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date], 
                       root_element, root_path, config_name,  
                       config_dict :dict[str, dict[str, str | None]], 
                       error_fields_set :set[str],
                       filename :str):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     FILENAME FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag == 'FILENAME':
            output_dict[field_tag] = filename

            
@typechecked
def do_basic_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date], 
                    root_element, root_path, config_name,  
                    config_dict :dict[str, dict[str, str | None] ], 
                    error_fields_set :set[str], 
                    pk_dict :dict[str, list[any]] ):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        type_tag = field_details_dict['config_type']
        if type_tag == 'FIELD':
            try:
                attribute_value = parse_field_from_dict(field_details_dict, root_element,
                                                    config_name, field_tag, root_path)
                output_dict[field_tag] = attribute_value
                logger.info(f"     FIELD for {config_name}/{field_tag} \"{attribute_value}\"")
            except KeyError as ke:
                logger.error(f"key erorr: {ke}")
                logger.error(f"  {field_details_dict}")
                logger.error(f"  FIELD for {config_name}/{field_tag} \"{attribute_value}\"")
                raise

        elif type_tag == 'PK':
            # PK fields are basically regular FIELDs that go into the pk_dict
            # NB. so do HASH fields.
            logger.info(f"     PK for {config_name}/{field_tag}")
            attribute_value = parse_field_from_dict(field_details_dict, root_element,
                                                    config_name, field_tag, root_path)
            output_dict[field_tag] = attribute_value
            pk_dict[field_tag].append(attribute_value)
            logger.info("PK {config_name}/{field_tag} {type(attribute_value)} {attribute_value}")
            

@typechecked 
def do_foreign_key_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date], 
                    root_element, root_path, config_name,  
                    config_dict :dict[str, dict[str, str | None] ], 
                    error_fields_set :set[str], 
                    pk_dict :dict[str, list[any]] ):
    """
        When a configuration has an FK field, it uses the tag in that configuration
        to find corresponding values from PK fields.  This mechanism is intended for
        PKs uniquely identified in a CCDA document header for any places in the sections
        it would be used as an FK. This is typically true for person_id and visit_occurrence_id, 
        but there are exceptions. In particular, some documents have multiple encounters, so
        you can't just naively choose the only visit_id because there are many.
        
        Choosing the visit is more complicated, because it requires a join (on date ranges)
        between the domain table and the encounters table, or portion of the header that
        has encompassingEncounters in it. This code, the do_foreign_key_fields() function
        operates in too narrow a context for that join. These functions are scoped down
        to processing a single config entry for a particular OMOP domain. The output_dict, 
        parameter is just for that one domain. It wouldn't include the encounters.
        For example, the measurement_results.py file has a configuration for parsing OMOP 
        measurement rows out of an XML file. The visit.py would have been previosly processed
        and it's rows stashed away elsewhere in the parse_doc() function whose scope is large
        enough to consider all the configurations. So the visit choice/reconcilliation
        must happen from there.
        
        TL;DR not all foreign keys are resolved here. In particular, domain FK references,
        visit_occurrence_id, in cases where more than a single encounter has previously been
        parsed, are not, can not, be resolved here. See the parse_doc() function for how
        it is handled there.
        
    """
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     FK config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        type_tag = field_details_dict['config_type']
        
        if type_tag == 'FK':
            logger.info(f"     FK for {config_name}/{field_tag}")
            if field_tag in pk_dict and  len(pk_dict[field_tag]) > 0:
                if len(pk_dict[field_tag]) == 1:
                    output_dict[field_tag] = pk_dict[field_tag][0]
                else:
                    # can't really choose the correct value here. Is attempted in reconcile_visit_FK_with_specific_domain() later, below.
                    ###print(f"WARNING FK has more than one value {field_tag}, tagging with 'RECONCILE FK' ")
                    logger.info(f"WARNING FK has more than one value {field_tag}, tagging with 'RECONCILE FK'")
                    # original hack:
                    output_dict[field_tag] = 'RECONCILE FK'
                
            else:
                path = root_path + "/"
                if 'element' in field_details_dict:
                    path = path + field_details_dict['element'] + "/@"
                else:
                    path = path + "no element/"
                if 'attribute' in field_details_dict:
                    path = path + field_details_dict['attribute']
                else:
                    path = path + "no attribute/"

##                if field_tag in pk_dict and len(pk_dict[field_tag]) == 0:
###                    logger.error(f"FK no value for {field_tag}  in pk_dict for {config_name}/{field_tag}")
##                else:
##                    logger.error(f"FK could not find {field_tag}  in pk_dict for {config_name}/{field_tag}")
                output_dict[field_tag] = None
                error_fields_set.add(field_tag)

@typechecked
def do_derived_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date], 
                      root_element, root_path, config_name,  
                      config_dict :dict[str, dict[str, str | None]], 
                      error_fields_set :set[str]):
    """ Do/compute derived values now that their inputs should be available in the output_dict
        Except for a special argument named 'default', when the value is what is other wise the field to look up in the output dict.
    """
    for (field_tag, field_details_dict) in config_dict.items():
        if field_details_dict['config_type'] == 'DERIVED':
            logger.info(f"     DERIVING {field_tag}, {field_details_dict}")
            # NB Using an explicit dict here instead of kwargs because this code here
            # doesn't know what the keywords are at 'compile' time.
            args_dict = {}
            for arg_name, field_name in field_details_dict['argument_names'].items():
                if arg_name == 'default':
                        args_dict[arg_name] = field_name
                else:
                    logger.info(f"     -- {field_tag}, arg_name:{arg_name} field_name:{field_name}")
                    if field_name not in output_dict:
                        error_fields_set.add(field_tag)
                        logger.error((f"DERIVED config:{config_name} field:{field_tag} could not "
                                      f"find {field_name} in {output_dict}"))
                    try:
                        args_dict[arg_name] = output_dict[field_name]
                    except Exception as e:
                        #print(traceback.format_exc(e))
                        error_fields_set.add(field_tag)
                        logger.error((f"DERIVED {field_tag} arg_name: {arg_name} field_name:{field_name}"
                                      f" args_dict:{args_dict} output_dict:{output_dict}"))
                        logger.error(f"DERIVED exception {e}")

            try:
                function_reference = field_details_dict['FUNCTION']
                function_value = field_details_dict['FUNCTION'](args_dict)
#                if function_reference != VT.concat_fields and function_value is None:
#                    logger.error((f"do_derived_fields(): No mapping back for {config_name} {field_tag}"
 #                                 f" from {field_details_dict['FUNCTION']}  {args_dict}   {config_dict[field_tag]}  "
#                                  "If this is from a value_as_concept/code field, it may not be an error, but "
 #                                 "an artificat of data that doesn't have a value or one that is not "
#                                  "meant as a concept id"))
                output_dict[field_tag] = function_value
                logger.info((f"     DERIVED {function_value} for "
                                f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))
            except KeyError as e:
                #print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED key error on: {e}")
                logger.error(f"DERIVED KeyError {field_tag} function can't find key it expects in {args_dict}")
                output_dict[field_tag] = None
            except TypeError as e:
                #print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED type error exception: {e}")
                logger.error((f"DERIVED TypeError {field_tag} possibly calling something that isn't a function"
                              " or that function was passed a null value." 
                              f" {field_details_dict['FUNCTION']}. You may have quotes "
                              "around it in  a python mapping structure if this is a "
                              f"string: {type(field_details_dict['FUNCTION'])}"))
                output_dict[field_tag] = None
            except Exception as e:
                logger.error(f"DERIVED exception: {e}")
                output_dict[field_tag] = None
            except Error as er:
                logger.error(f"DERIVED error: {e}")
                output_dict[field_tag] = None
                
@typechecked
def do_domain_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date], 
                     root_element, root_path, config_name, 
                     config_dict :dict[str, dict[str, str | None]], 
                     error_fields_set :set[str]) -> str | None :
    # nearly the same as derived above, but returns the domain for later filtering
    domain_id = None
    have_domain_field = False
    for (field_tag, field_details_dict) in config_dict.items():
        if field_details_dict['config_type'] == 'DOMAIN':
            have_domain_field = True
            logger.info(f"     Deriving DOMAIN {field_tag}, {field_details_dict}")

            # Collect args for the function
            args_dict = {}
            for arg_name, field_name in field_details_dict['argument_names'].items():
                if arg_name == 'default':
                        args_dict[arg_name] = field_name
                else:
                    logger.info(f"     -- {field_tag}, arg_name:{arg_name} field_name:{field_name}")
                    if field_name not in output_dict:
                        error_fields_set.add(field_tag)
                        logger.error((f"DOMAIN config:{config_dict} field:{field_tag} could not "
                                      f"find {field_name} in {output_dict}"))
                    try:
                        args_dict[arg_name] = output_dict[field_name]
                    except Exception:
                        error_fields_set.add(field_tag)
                        logger.error((f"DOMAIN {field_tag} arg_name: {arg_name} field_name:{field_name}"
                                      f" args_dict:{args_dict} output_dict:{output_dict}"))
                    except Exception as e:
                        logger.error(f"DERIVED exception: {e}")
                        output_dict[field_tag] = None
                    except Error as er:
                        logger.error(f"DERIVED error: {e}")
                        output_dict[field_tag] = None
                    
            # Derive the value
            try:
                function_reference = field_details_dict['FUNCTION']
                function_value = field_details_dict['FUNCTION'](args_dict)
                if function_reference != VT.concat_fields and (function_value is None or function_value == 0): 
                    logger.error((f"do_domain_fields(): No mapping back for {config_name} {field_tag} "
                                  f"from {field_details_dict['FUNCTION']} {args_dict}   {config_dict[field_tag]}"
                                  "If this is from a value_as_concept/code field, it may not be an error, but "
                                  "an artificat of data that doesn't have a value or one that is not "
                                  "meant as a concept id"))
                domain_id = function_value
                output_dict[field_tag] = function_value
                logger.info((f"     DOMAIN captured as {function_value} for "
                                 f"{field_tag}, {field_details_dict}"))
            except KeyError as e:
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED exception: {e}")
                logger.error(f"DERIVED {field_tag} can't find argument in {args_dict}")
            except TypeError as e:
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED exception: {e}")
                logger.error((f"DERIVED {field_tag} possibly calling something that isn't a function"
                              f" {field_details_dict['FUNCTION']}. You may have quotes "
                              "around it in  a python mapping structure if this is a "
                              f"string: {type(field_details_dict['FUNCTION'])}"))
                output_dict[field_tag] = None
            except Exception as e:
                logger.error(f"DERIVED exception: {e}")
                output_dict[field_tag] = None
            except Error as er:
                logger.error(f"DERIVED error: {e}")
                output_dict[field_tag] = None

    if domain_id == 0: # TODO, we should decide between 0/NMC and None for an unknown domain_id
        ###print(f"DEBUG got 0 for a domain_id, returning None in do_domain_fields(). {config_name}")
        if not have_domain_field:
            logger.error(f"ERROR didn't find a field of type DOMAIN in config {config_name}.")
            print(f"ERROR didn't find a field of type DOMAIN in config {config_name}")
        else:
            logger.error(f"ERROR didn't get a DOMAIN value in config {config_name}, check if the concept maps have this concept.")
        return None
    else:
        return domain_id


@typechecked
def do_hash_fields(output_dict :dict[str, None | str | float | int | int32 | datetime.datetime | datetime.date], 
                   root_element, root_path, config_name,  
                   config_dict :dict[str, dict[str, str | None]], 
                   error_fields_set :set[str], 
                   pk_dict :dict[str, list[any]]):
    """ These are basically derived, but the argument is a lsit of field names, instead of
        a fixed number of individually named fields.
        Dubiously useful in an environment where IDs are  32 bit integers.
        See the code above for converting according to the data_type attribute
        where a different kind of hash is beat into an integer.
    """
    for (field_tag, field_details_dict) in config_dict.items():
        if field_details_dict['config_type'] == 'HASH':
            value_list = []
            if 'fields' not in field_details_dict:
                logger.error(f"HASH field {field_tag} is missing 'fields' attributes in config:{config_name}")
            for field_name in field_details_dict['fields'] :
                if field_name in output_dict:
                    value_list.append(output_dict[field_name])
            hash_input =  "|".join(map(str, value_list))
            hash_value = create_hash(hash_input)
            output_dict[field_tag] = hash_value
            # treat as PK and include in that dictionary
            pk_dict[field_tag].append(hash_value)
            logger.info((f"     HASH (PK) {hash_value} for "
                         f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))

            
@typechecked
def do_priority_fields(output_dict :dict[str, None | str | float | int | int32 |  datetime.datetime | datetime.date], 
                       root_element, root_path, config,  
                       config_dict :dict[str, dict[str, str | None]], 
                       error_fields_set :set[str], 
                       pk_dict :dict[str, list[any]]) -> dict[str, list]:
    """
        Returns the list of  priority_names so the chosen one (first non-null) can be 
        added to output fields Also, adds this field to the PK list?
        This is basically what SQL calls a coalesce.

        Within the config_dict, find all fields tagged with priority and group
        them by their priority names in a dictionary keyed by that name
        Ex. { 'person_id': [ ('person_id_ssn', 1), ('person_id_unknown', 2) ]
        Sort them, choose the first one that is not None.

        NB now there is a separate config_type PRIORITY to compliment the priority attribute.
        So you might have person_id_npi, person_id_ssn and person_id_hash tagged with priority
        attributes to create a field person_id, but then also another field, just plain person_id.
        The point of it is to have a unique place to put that field's order attribute. The code
        here (and in the ordering code later) must be aware of a  that field in the
        config_dict (where it isn't used) ...and not clobber it. It's an issue over in the
        sorting/ordering.
    """

    # Create Ref Data
    # for each new field, create a list of source fields and their priority:
    # Ex. [('person_id_other', 2), ('person_id_ssn', 1)]
    priority_fields = {}
    for field_key, config_parts in config_dict.items():
        if  'priority' in config_parts:
            new_field_name = config_parts['priority'][0]
            if new_field_name in priority_fields:
                priority_fields[new_field_name].append( (field_key, config_parts['priority'][1]))
            else:
                priority_fields[new_field_name] = [ (field_key, config_parts['priority'][1]) ]

    # Choose Fields
    # first field in each set with a non-null value in the output_dict adds that value to the dict with it's priority_name
    for priority_name, priority_contents in priority_fields.items():
        sorted_contents = sorted(priority_contents, key=lambda x: x[1])
        # Ex. [('person_id_ssn', 1), ('person_id_other, 2)]

        found=False
        for value_field_pair in sorted_contents: 
            if value_field_pair[0] in output_dict and output_dict[value_field_pair[0]] is not None:
                output_dict[priority_name] = output_dict[value_field_pair[0]]
                pk_dict[priority_name].append(output_dict[value_field_pair[0]])
                found=True
                break

        if not found:
            # relent and put a None if we didn't find anything
            output_dict[priority_name] = None
            pk_dict[priority_name].append(None)

    return priority_fields
    
    
@typechecked
def get_extract_order_fn(dict):
    def get_order_from_dict(field_key):
        if 'order' in dict[field_key]:
            logger.info(f"{field_key} {dict[field_key]['order']}")
            return int(dict[field_key]['order'])
        else:
            logger.info(f"extract_order_fn, no order in {field_key}")
            return int(sys.maxsize)

    return get_order_from_dict


@typechecked
def get_filter_fn(dict):
    def has_order_attribute(key):
        return 'order' in dict[key] and dict[key]['order'] is not None
    return has_order_attribute


@typechecked
def sort_output_dict(output_dict :dict[str, None | str | float | int], 
                     config_dict :dict[str, dict[str, str | None]], config_name):
    """ Sorts the ouput_dict by the value of the 'order' fields in the associated
        config_dict. Fields without a value, or without an entry used to 
        come last, now are omitted.
    """
    ordered_output_dict = {}

    sort_function = get_extract_order_fn(config_dict) # curry in the config_dict arg.
    ordered_keys = sorted(config_dict.keys(), key=sort_function)

    filter_function = get_filter_fn(config_dict)
    filtered_ordered_keys = filter(filter_function, ordered_keys)

    for key in filtered_ordered_keys:
        if key in output_dict:
            ordered_output_dict[key] = output_dict[key]

    return ordered_output_dict


@typechecked
def parse_config_for_single_root(root_element, root_path, config_name, 
                                 config_dict :dict[str, dict[str, str | None]], 
                                 error_fields_set : set[str], 
                                 pk_dict :dict[str, list[any]],
                                 filename :str) -> dict[str,  None | str | float | int | datetime.datetime | datetime.date] | None:

    """  Parses for each field in the metadata for a config out of the root_element passed in.
         You may have more than one such root element, each making for a row in the output.

        If the configuration includes a field of config_type DOMAIN, the value it generates
        will be compared to the domain specified in the config in. If they are different, null is returned.
        This is how  OMOP "domain routing" is implemented here.


         Returns (output_dict,  error_fields_set)
    """
    output_dict = {} #  :dict[str, any]  a record
    domain_id = None
    logger.info((f"  ROOT for config:{config_name}, we have tag:{root_element.tag}"
                 f" attributes:{root_element.attrib}"))

    do_none_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_constant_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_filename_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, filename)
    do_basic_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    do_derived_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    domain_id = do_domain_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_hash_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    priority_field_names = do_priority_fields(output_dict, root_element, root_path, config_name,  config_dict,
                                              error_fields_set, pk_dict)
    do_foreign_key_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    output_dict = sort_output_dict(output_dict, config_dict, config_name)


    expected_domain_id = config_dict['root']['expected_domain_id']
    if (expected_domain_id == domain_id or domain_id is None):
        if expected_domain_id == "Observation":
            logger.warning((f"ACCEPTING {domain_id} "
                            f"id:{output_dict['observation_id']} "
                            f"cpt:{output_dict['observation_concept_id']}" ) )
        elif expected_domain_id == "Measurement":
            logger.warning((f"ACCEPTING {domain_id} "
                            f"id:{output_dict['measurement_id']} "
                            f"cpt:{output_dict['measurement_concept_id']}") )
        elif expected_domain_id == "Procedure":
            logger.warning((f"ACCEPTING {domain_id} "
                            f"id:{output_dict['procedure_occurrence_id']} "
                            f"cpt:{output_dict['procedure_concept_id']}") )
        return output_dict
    else:
        if expected_domain_id == "Observation":
            logger.warning((f"DENYING/REJECTING have:{domain_id} domain:{expected_domain_id} "
                            f"id:{output_dict['observation_id']} "
                            f"cpt:{output_dict['observation_concept_id']}" ))
        elif expected_domain_id == "Measurement":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['measurement_id']} "
                              f"cpt:{output_dict['measurement_concept_id']}") )
        elif expected_domain_id == "Procedure":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['procedure_occurrence_id']} "
                              f"cpt:{output_dict['procedure_concept_id']}") )
        elif expected_domain_id == "Drug":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['drug_exposure_id']} "
                              f"cpt:{output_dict['drug_concept_id']}") )
        elif expected_domain_id == "Condition":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['condition_occurrencd_id']} "
                              f"cpt:{output_dict['condition_concept_id']}") )
        else:
            logger.warning((f"DENYING/REJECTING have:{domain_id} domain:{expected_domain_id} "))
        return None


@typechecked
def parse_config_from_xml_file(tree, config_name, 
                           config_dict :dict[str, dict[str, str | None]], filename, 
                           pk_dict :dict[str, list[any]]) -> list[ dict[str,  None | str | float | int | datetime.datetime | datetime.date] | None  ] | None:
                                                                   
    """ The main logic is here.
        Given a tree from ElementTree representing a CCDA document
        (ClinicalDocument, not just file),
        parse the different domains out of it (1 config each), linking PK and FKs between them.

        Returns a list, output_list, of dictionaries, output_dict, keyed by field name,
        containing a list of the value and the path to it:
            [ { field_1 : (value, path), field_2: (value, path)},
              { field_1: (value, path)}, {field_2: (value, path)} ]
        It's a list of because you might have more than one instance of the root path, like when you
        get many observations.
        
        arg: tree, this is the lxml.etree parse of the XML file
        arg: config_name, this is a key into the first level of the metadata, an often a OMOP domain name
        arg: config_dict, this is the value of that key in the dict
        arg: filename, the name of the XML file, for logging
        arg: pk_dict, a dictionary for Primary Keys, the keys here are field names and 
             their values are their values. It's a sort of global space for carrying PKs 
             to other parts of processing where they will be used as FKs. This is useful
             for things like the main person_id that is part of the context the document creates.


    """

    # log to a file per file/config
    base_name = os.path.basename(filename)
    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"logs/log_config_{base_name}_{config_name}.log",
        #force=True, level=logging.WARNING)
        force=True, level=logging.ERROR)

    # Find root
    if 'root' not in config_dict:
        logger.error(f"CONFIG {config_dict} lacks a root element.")
        return None

    if 'element' not in config_dict['root']:
        logger.error(f"CONFIG {config_dict} root lacks an 'element' key.")
        return None

    root_path = config_dict['root']['element']
    logger.info((f"CONFIG >>  config:{config_name} root:{config_dict['root']['element']}"
                 f"   ROOT path:{root_path}"))
    #root_element_list = tree.findall(config_dict['root']['element'], ns)
    root_element_list = None
    try:
        root_element_list = tree.xpath(config_dict['root']['element'], namespaces=ns)
    except Exception as e:
        logger.error(f" {config_dict['root']['element']}   {e}")
        
    if root_element_list is None or len(root_element_list) == 0:
#        logger.error((f"CONFIG couldn't find root element for {config_name}"
#                      f" with {config_dict['root']['element']}"))
        return None

    output_list = []
    error_fields_set = set()
    logger.info(f"NUM ROOTS {config_name} {len(root_element_list)}")
    for root_element in root_element_list:
        output_dict = parse_config_for_single_root(root_element, root_path, 
                config_name, config_dict, error_fields_set, pk_dict, filename)
        if output_dict is not None:
            output_list.append(output_dict)

    # report fields with errors
    if len(error_fields_set) > 0:
        print(f"DOMAIN Fields with errors in config {config_name} {error_fields_set}")
        logger.error(f"DOMAIN Fields with errors in config {config_name} {error_fields_set}")

    return output_list

                          
                          
#                          
##################################################
#
           
                          
""" domain_dates tell the FK functionality in do_foreign_keys() how to 
    choose visits for domain_rows.It is one of the most encumbered parts of the code.
    
    Rules:
    - Encounters must be populated before domains. This is controlled by the
      order of the metadata files in the metadata/__init__.py file.
    - This structure must include a mapping from start or start and end to
      names of the fields for each specific domain to be processed.
    - These are _config_ names, not domain names. For example, the domain
      Measurement is fed by configs names Measurement_vital_signs, and 
      Measurement_results. They are the keys into the output dict where the
      visit candidates will be found.
    + This all happens in the do_basic_keys 
    
    Background: An xml file is processed in phases, one for each configuration file in 
    the metadata directory. Since the configuration files are organized by omop table,
    it's helpful to think of the phases being the OMOP tables too.  Within each config 
    phase, there is another level of phases: the types of the fields: none, constant, 
    basic, derived, domain, hash, and foreign key. This means any fields in the current 
    config phase are available for looking up the value of a foreign key.
    
"""
domain_dates = {
    'Measurement': {'date': ['measurement_date', 'measurement_datetime'],
                    'id': 'measurement_id'},
    'Observation': {'date': ['observation_date', 'observation_datetime'],
                    'id': 'observation_id'},
    'Condition'  : {'start': ['condition_start_date', 'condition_start_datetime'], 
                    'end':   ['condition_end_date', 'condition_end_datetime'],
                    'id': 'condition_id'},
    'Procedure'  : {'date': ['procedure_date', 'procedure_datetime'],
                    'id': 'procedure_occurrence_id'},
    'Drug'       : {'start': ['drug_exposure_start_date', 'drug_exposure_start_datetime'],
                    'end': ['drug_exposure_end_date', 'drug_exposure_end_datetime'],
                    'id': 'drug_exposure_id'},
}
        
@typechecked 
def reconcile_visit_FK_with_specific_domain(domain: str, 
                                            domain_dict: list[dict[str, None | str | float | int | datetime.datetime | datetime.date] ] | None , 
                                            visit_dict:  list[dict[str, None | str | float | int | datetime.datetime | datetime.date] ] | None):
    if visit_dict is None:
        logger.error(f"no visits for {domain} in reconcile_visit_FK_with_specific_domain, reconcilliation")
        return

    if domain_dict is None:
        logger.error(f"no data for {domain} in reconcile_visit_FK_with_specific_domain, reconcilliation")
        return
    
    if domain not in domain_dates:
        logger.error(f"no metadata for domain {domain} in reconcile_visit_FK_with_specific_domain, reconcilliation")
        
     # Q: does this domain NEED to have it's visit foreign key reconcileed??!!!! TODO
     # (in this project things are called a domain and passed in here that are not really domains in OMOP, locations, providers, care-sites, etc. )

     # Q: do they have different IDs??? (not in Patient-502.xml)
     # A: it will get caught in the PK constraints when loaded to duckdb

     # Q: do these strings parse, and so compare as dates??? 
     # A: All parsing done upstream with datatype constraints on the FIELD configs
        
    if 'date' in domain_dates[domain].keys():
        # just one date
            for thing in domain_dict:

                date_field_name = domain_dates[domain]['date'][0]
                datetime_field_name = domain_dates[domain]['date'][1]
                # Q: what if you have dates in one place and datetimes in the other? TODO
                # A: work with the most-specific value you have.
                # TODO: make a test case that has different combinations, check that parsing populates them.
                date_field_value = thing[date_field_name] 
                if thing[datetime_field_name] is not None:
                    date_field_value = thing[datetime_field_name]
                
                
                # TODO: check for a second visit that fits
                # TODO: the bennis_shauna file has UNK for the end date.
                
                if date_field_value is not None :
                    # compare dates
                                    
                    have_visit = False
                    for visit in visit_dict:
                        try:
                            start_visit_date = visit['visit_start_date']
                            if visit['visit_start_datetime'] is not None:
                                start_visit_date = visit['visit_start_datetime']
                                
                            end_visit_date = visit['visit_end_date']
                            if visit['visit_end_datetime'] is not None:
                                end_visit_date = visit['visit_end_datetime']
                            
                            # Normalize all to datetime.date
                            #if isinstance(start_visit_date, datetime.datetime):
                            #    start_visit_date = start_visit_date.date()
                            
                            #if isinstance(end_visit_date, datetime.datetime):
                            #    end_visit_date = end_visit_date.date()
                                
                            #if isinstance(date_field_value, datetime.datetime):
                            #    date_field_value = date_field_value.date()

                            # Remove timezone info by converting all to naive datetime
                            if start_visit_date.tzinfo is not None:
                                start_visit_date = start_visit_date.replace(tzinfo=None)
                            
                            if end_visit_date.tzinfo is not None:
                                end_visit_date = end_visit_date.replace(tzinfo=None)
                                
                            if date_field_value.tzinfo is not None:
                                date_field_value = date_field_value.replace(tzinfo=None)
                            
                            if start_visit_date <= date_field_value and date_field_value <= end_visit_date:
                                ###print(f"MATCHED visit: v_start:{start_visit_date} d_date:{date_field_value} v_end:{end_visit_date}")
                                # got one! ....is it the first?
                                if not have_visit:
                                    # update the visit_occurrence_id in that domain record
                                    thing['visit_occurrence_id'] = visit['visit_occurrence_id']
                                ###else:
                                    ###print("WARNING got a second fitting visit for {domain} {thing[domain_dates['id']]}")
                        except KeyError as ke:
                           logger.error(f"missing field  \"{ke}\", in visit reconcilliation, see warnings for detail")
                           logger.warning(f"missing field  \"{ke}\", in visit reconcilliation, got error {type(ke)} ")
                        except Exception as e:
                            pass
#                            logger.error(f"something wrong in visit reconciliation \"{e}\" see warnings for detail ")
#                            logger.warning(f"something wrong in visit reconciliation \"{e}\" {type(e)} ")
                    ###if not have_visit:
                        ###print(f"WARNING wasn't able to reconcile {domain} {thing}")
                        ###print("")
                        
                else:
                    # S.O.L.
                    ### print(f"ERROR no date available for visit reconcilliation in domain {domain} (detail in logs)")
                    logger.error(f"no date available for visit reconcilliation in domain {domain} for {thing}")
                

    elif 'start' in domain_dates[domain].keys() and 'end' in domain_dates[domain].keys():
        for thing in domain_dict:
            start_date_field_name = domain_dates[domain]['start'][0]
            start_datetime_field_name = domain_dates[domain]['start'][1]
            end_date_field_name = domain_dates[domain]['end'][0]
            end_datetime_field_name = domain_dates[domain]['end'][1]
            
            start_date_value = thing[start_date_field_name]
            end_date_value = thing[end_date_field_name]
            
            if thing[start_datetime_field_name] is not None:
                    start_date_value = thing[start_datetime_field_name]
                    
            if thing[end_datetime_field_name] is not None:
                    end_date_value = thing[end_datetime_field_name]
            
            if start_date_value is not None and end_date_value is not None:
                have_visit = False
                for visit in visit_dict:
                    try:
                        start_visit_date = visit['visit_start_date']
                        if visit['visit_start_datetime'] is not None:
                            start_visit_date = visit['visit_start_datetime']
                                
                        end_visit_date = visit['visit_end_date']
                        if visit['visit_end_datetime'] is not None:
                            end_visit_date = visit['visit_end_datetime']
                        
                        if start_visit_date and end_visit_date:
                            # Check if event overlaps with the visit period
                            if (
                                (start_visit_date <= start_date_value <= end_visit_date) or
                                (start_visit_date <= end_date_value <= end_visit_date) or
                                (start_date_value <= start_visit_date and end_visit_date <= end_date_value)
                            ):
###                                print(f"MATCHED visit: v_start:{start_visit_date} event_start:{start_date_value} event_end:{end_date_value} v_end:{end_visit_date}")
                                if not have_visit:
                                    thing['visit_occurrence_id'] = visit['visit_occurrence_id']
                                else:
                                    print(f"WARNING multiple fitting visits for {domain} {thing[domain_dates['id']]}")
                    except KeyError as ke:
                        print(f"WARNING missing field  \"{ke}\", in visit reconcilliation, got error {type(ke)} ")   
                    except Exception as e:
                        print(f"WARNING something wrong in visit reconciliation: {e}")

                if not have_visit:
                    logger.error(f" couldn't reconcile visit for {domain} event: {thing}")
                    ##print(f"WARNING couldn't reconcile visit for {domain} event: {thing}")
                    #print("")
            
            else:
                    # S.O.L.
                    ###print(f"ERROR no date available for visit reconcilliation in domain {domain} (detail in logs)")
                    logger.error(f" no date available for visit reconcilliation in domain {domain} for {thing}")

    else:
        logger.error("??? bust in domain_dates for reconcilliation")
        

    
    
@typechecked
def reconcile_visit_foreign_keys(data_dict :dict[str, 
                                                 list[ dict[str,  None | str | float | int | datetime.datetime | datetime.date] | None  ] | None]) :
    # data_dict is a dictionary of config_names to a list of record-dicts
    metadata = [
    ('Measurement', 'Measurement_results', 'Visit' ),
    ('Measurement', 'Measurement_vital_signs', 'Visit' ),
    ('Observation', 'Observation', 'Visit' ),
    ('Condition', 'Condition', 'Visit' ),
    ('Procedure', 'Procedure_activity_procedure', 'Visit'),
    ('Procedure', 'Procedure_activity_observation', 'Visit'),
    ('Procedure', 'Procedure_activity_act', 'Visit'),
    ('Drug', 'Medication_medication_activity', 'Visit'),
    ('Drug', 'Medication_medication_dispense', 'Visit'),
    ('Drug', 'Immunization_immunization_activity', 'Visit')
    ]

    for meta_tuple in metadata:
        #print(f" reconciling {meta_tuple[1]}")
        reconcile_visit_FK_with_specific_domain(meta_tuple[0], data_dict[meta_tuple[1]], data_dict[meta_tuple[2]] )
                          
                          
@typechecked
def parse_doc(file_path, 
              metadata :dict[str, dict[str, dict[str, str]]]) -> dict[str, 
                      list[ dict[str,  None | str | float | int] | None  ] | None]:
    """ Parses many meta configs from a single file, collects them in omop_dict.
        Returns omop_dict, a  dict keyed by configuration names, 
          each a list of record/row dictionaries.
    """
    omop_dict = {}
    pk_dict = defaultdict(list)
    tree = ET.parse(file_path)
    base_name = os.path.basename(file_path)
    for config_name, config_dict in metadata.items():
#        print(f" {base_name} {config_name}")
        data_dict_list = parse_config_from_xml_file(tree, config_name, config_dict, base_name, pk_dict)
        if config_name in omop_dict: 
            omop_dict[config_name] = omop_dict[config_name].extend(data_dict_list)
        else:
            omop_dict[config_name] = data_dict_list
            
        #if data_dict_list is not None:
        #    print(f"...PARSED, got {len(data_dict_list)}")
        #else:
        #    print(f"...PARSED, got **NOTHING** {data_dict_list} ")
    return omop_dict


@typechecked
def print_omop_structure(omop :dict[str, list[ dict[str, None | str | float | int ] ] ], 
                         metadata :dict[str, dict[str, dict[str, str ] ] ] ):
    
    """ prints a dict of parsed domains as returned from parse_doc()
        or parse_domain_from_dict()
    """
    for domain, domain_list in omop.items():
        if domain_list is None:
            logger.warning(f"no data for domain {domain}")
        else:
            for domain_data_dict in domain_list:
                n = 0
                if domain_data_dict is None:
                    print(f"\n\nERROR DOMAIN: {domain} is NONE")
                else:
                    print(f"\n\nDOMAIN: {domain} {domain_data_dict.keys()} ")
                    for field, parts in domain_data_dict.items():
                        print(f"    FIELD:{field}")
                        #print(f"        parts type {type(parts[0])}")
                        #print(f"        parts type {type(parts[1])}")
                        print(f"        parts type {type(parts)}")
                        print(f"        VALUE:{parts}")
                        #print(f"        VALUE:{parts[0]}")
                        #print(f"        PATH:{parts[1]}")
                        print(f"        ORDER: {metadata[domain][field]['order']}")
                        n = n+1
                    print(f"\n\nDOMAIN: {domain} {n}\n\n")

                    
@typechecked
def process_file(filepath :str, print_output: bool):
    """ Process each configuration in the metadata for one file.
        Returns nothing.
        Prints the omop_data. See better functions in layer_datasets.puy
    """
    print(f"PROCESSING {filepath} ")
    logger.info(f"PROCESSING {filepath} ")
    base_name = os.path.basename(filepath)

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"logs/log_file_{base_name}.log",
        force=True,
        # level=logging.ERROR
        level=logging.WARNING
        # level=logging.INFO
        # level=logging.DEBUG
    )

    metadata = get_meta_dict()
    print(f"    {filepath} parse_doc() ")
    omop_data = parse_doc(filepath, metadata)
    print(f"    {filepath} reconcile_visit()() ")
    reconcile_visit_foreign_keys(omop_data)
    if print_output and (omop_data is not None or len(omop_data) < 1):
        print_omop_structure(omop_data, metadata)
    else:
        logger.error(f"FILE no data from {filepath} (or printing turned off)")

    print(f"done PROCESSING {filepath} ")


# for argparse
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def main() :
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
    parser.add_argument('-p', '--print_output', 
            type=str2bool, const=True, default=True,  nargs="?",
            help="print out the output values, -p False to have it not print")
    args = parser.parse_args()

    if args.filename is not None:
        process_file(args.filename, args.print_output)
    elif args.directory is not None:
        only_files = [f for f in os.listdir(args.directory) if os.path.isfile(os.path.join(args.directory, f))]
        for file in (only_files):
            if file.endswith(".xml"):
            	process_file(os.path.join(args.directory, file), args.print_output)
    else:
        logger.error("Did args parse let us  down? Have neither a file, nor a directory.")


if __name__ == '__main__':
    main()
