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

D
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
    
    output_dict :dict[str, tuple[any, str]
    omop_dict : dict[str, list[tuple[any, str]] for each config you have a list of records
    


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
import logging
import os
import sys
import hashlib
import zlib
import ctypes
import traceback
from lxml import etree as ET
from lxml.etree import XPathEvalError
###from typing import dict, List, Tuple
from typeguard import typechecked
#https://typeguard.readthedocs.io/en/latest/userguide.html

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

def create_hash(input_string):
    """ matches common SQL code when that code also truncates to 13 characters
        SQL: cast(conv(substr(md5(test_string), 1, 15), 16, 10) as bigint) as hashed_value
    """
    if input_string == '':
        return None
    
    hash_value = hashlib.md5(input_string.encode('utf-8').upper())
    truncated_hash = hash_value.hexdigest()[0:13]
    int_trunc_hash_value = int(truncated_hash, 16)
    return int_trunc_hash_value


def cast_to_date(string_value):
    # TODO does CCDA always do dates as YYYYMMDD ?
    # TODO  when  is it date and when datetime

    # Step 1 : put dashes in  to make ISO-8601 happy, b/c python insists on dashes.
    iso_8601_string = f"{string_value[0:4]}-{string_value[4:6]}-{string_value[6:8]}"
    #print(f"DATE iso8601 string {string_value} {iso_8601_string}")

    # Step 2: dont' both with fancy date classes that can't help us here.

    return iso_8601_string


def cast_to_datetime(string_value):
    # TODO does CCDA always do dates as YYYYMMDD ? ...without dashes?
    if len(string_value) > 8:
        iso_8601_string = f"{string_value[0:4]}-{string_value[4:6]}-{string_value[6:8]} {string_value[8:10]}:{string_value[10:12]}"
        #print(f"DATETIME iso8601 string {string_value}  {iso_8601_string}")
        return iso_8601_string
    else:
        #print(f"{string_value} too short for DATETIME")
        return cast_to_date(string_value)


@typechecked
def parse_field_from_dict(field_details_dict :dict[str, str], root_element, config_name, field_tag, root_path):
    """ Retrieves a value for the field descrbied in field_details_dict that lies below
        the root_element.
        Domain and field_tag are here for error messages.
    """

    if 'element' not in field_details_dict:
        logger.error(("FIELD could find key 'element' in the field_details_dict:"
                     f" {field_details_dict} root:{root_path}"))
        return None

    logger.info(f"    FIELD {field_details_dict['element']} for {config_name}/{field_tag}")
    field_element = None
    try:
        field_element = root_element.xpath(field_details_dict['element'], namespaces=ns)
    except XPathEvalError as p:
        logger.error("ERROR (often inconsequential) {field_details_dict['element']} {p}")
        print(f"FAILED often inconsequential  {field_details_dict['element']} {p}")
    if field_element is None:
        logger.error((f"FIELD could not find field element {field_details_dict['element']}"
                      f" for {config_name}/{field_tag} root:{root_path} {field_details_dict} "))
        return None

    if 'attribute' not in field_details_dict:
        logger.error((f"FIELD could not find key 'attribute' in the field_details_dict:"
                     f" {field_details_dict} root:{root_path}"))
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
                print((f"ERROR: no text elemeent for field element {field_element} "
                       f"for {config_name}/{field_tag} root:{root_path} "
                       f" dict: {field_element[0].attrib} EXCEPTION:{e}"))
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
                attribute_value = cast_to_date(attribute_value)
            elif field_details_dict['data_type'] == 'DATETIME':
                attribute_value = cast_to_datetime(attribute_value)
            elif field_details_dict['data_type'] == 'INTEGER':
                attribute_value = int(attribute_value)
            elif field_details_dict['data_type'] == '32BINTEGER':
                attribute_value = ctypes.c_int32(int(attribute_value)).value
            elif field_details_dict['data_type'] == 'BIGINTHASH':
                attribute_value = create_hash(attribute_value)
            elif field_details_dict['data_type'] == 'FLOAT':
                attribute_value = float(attribute_value)
            else:
                print(f"ERROR UNKNOWN DATA TYPE: {field_details_dict['data_type']}")
                logger.error(f" UNKNOWN DATA TYPE: {field_details_dict['data_type']} {config_name} {field_tag}")
            return attribute_value
        else:
            return None
    else:
        return attribute_value


@typechecked
def do_none_fields(output_dict :dict[str, tuple[any, str]], root_element, root_path, config_name,  
                   config_dict :dict[str, dict[str, str | None]], error_fields_set):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     NONE FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag is None:
            output_dict[field_tag] = (None, '(None type)')

            
@typechecked
def do_constant_fields(output_dict :dict[str, tuple[any, str]], root_element, root_path, config_name,  
                       config_dict :dict[str, dict[str, str | None]], error_fields_set):

    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     CONSTANT FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag == 'CONSTANT':
            constant_value = field_details_dict['constant_value']
            output_dict[field_tag] = (constant_value, '(None type)')

            
@typechecked
def do_basic_fields(output_dict :dict[str, tuple[any, str] ], root_element, root_path, config_name,  
                    config_dict :dict[str, dict[str, str | None] ], error_fields_set, 
                    pk_dict :dict[str, any] ):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        type_tag = field_details_dict['config_type']
        if type_tag == 'FIELD':
            try:
                attribute_value = parse_field_from_dict(field_details_dict, root_element,
                                                    config_name, field_tag, root_path)
                output_dict[field_tag] = (attribute_value, root_path + "/" +
                                      field_details_dict['element'] + "/@" +
                                      field_details_dict['attribute'])
                logger.info(f"     FIELD for {config_name}/{field_tag} \"{attribute_value}\"")
            except KeyError as ke:
                print(f"ERROR      key erorr: {ke}")
                print(f"  {field_details_dict}")
                print(f"  FIELD for {config_name}/{field_tag} \"{attribute_value}\"")
                logger.error(f"key erorr: {ke}")
                logger.error(f"  {field_details_dict}")
                logger.error(f"  FIELD for {config_name}/{field_tag} \"{attribute_value}\"")
                raise

        elif type_tag == 'PK':
            logger.info(f"     PK for {config_name}/{field_tag}")
            attribute_value = parse_field_from_dict(field_details_dict, root_element,
                                                    config_name, field_tag, root_path)
            output_dict[field_tag] = (attribute_value, root_path + "/" +
                                      field_details_dict['element'] + "/@" +
                                      field_details_dict['attribute'])
            pk_dict[field_tag] = attribute_value
        elif type_tag == 'FK':
            logger.info(f"     FK for {config_name}/{field_tag}")
            if field_tag in pk_dict:
                output_dict[field_tag] = (pk_dict[field_tag], 'FK')
            else:
                logger.error(f"FK could not find {field_tag}  in pk_dict for {config_name}/{field_tag}")
                path = root_path + "/"
                if 'element' in field_details_dict:
                    path = path + field_details_dict['element'] + "/@"
                else:
                    path = path + "no element/"
                if 'attribute' in field_details_dict:
                    path = path + field_details_dict['attribute']
                else:
                    path = path + "no attribute/"
                output_dict[field_tag] = (None, path)
                error_fields_set.add(field_tag)

                
@typechecked
def do_derived_fields(output_dict :dict[str, tuple[any, str]], root_element, root_path, config_name,  
                      config_dict :dict[str, dict[str, str | None]], error_fields_set):
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
                        args_dict[arg_name] = output_dict[field_name][0]
                    except Exception as e:
                        print(traceback.format_exc(e))
                        error_fields_set.add(field_tag)
                        logger.error((f"DERIVED {field_tag} arg_name: {arg_name} field_name:{field_name}"
                                      f" args_dict:{args_dict} output_dict:{output_dict}"))
                        logger.error(f"DERIVED exception {e}")

            try:
                function_value = field_details_dict['FUNCTION'](args_dict)
                output_dict[field_tag] = (function_value, 'DERIVED')
                logger.info((f"     DERIVED {function_value} for "
                                f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))
            except KeyError as e:
                print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED exception: {e}")
                logger.error(f"DERIVED KeyError {field_tag} function can't find key it expects in {args_dict}")
                output_dict[field_tag] = (None, field_details_dict['config_type'])
            except TypeError as e:
                print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED exception: {e}")
                logger.error((f"DERIVED TypeError {field_tag} possibly calling something that isn't a function"
                              " or that function was passed a null value." 
                              f" {field_details_dict['FUNCTION']}. You may have quotes "
                              "around it in  a python mapping structure if this is a "
                              f"string: {type(field_details_dict['FUNCTION'])}"))
                output_dict[field_tag] = (None, field_details_dict['config_type'])

                
@typechecked
def do_domain_fields(output_dict :dict[str, tuple[any, str]], root_element, root_path, config_name, 
                     config_dict :dict[str, dict[str, str | None]], error_fields_set) -> str | None :
    # nearly the same as derived above, but returns the domain for later filtering
    domain_id = None
    for (field_tag, field_details_dict) in config_dict.items():
        if field_details_dict['config_type'] == 'DOMAIN':
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
                        logger.error((f"DERIVED config:{config_dict} field:{field_tag} could not "
                                      f"find {field_name} in {output_dict}"))
                    try:
                        args_dict[arg_name] = output_dict[field_name][0]
                    except Exception:
                        error_fields_set.add(field_tag)
                        logger.error((f"DERIVED {field_tag} arg_name: {arg_name} field_name:{field_name}"
                                      f" args_dict:{args_dict} output_dict:{output_dict}"))
            # Derive the value
            try:
                function_value = field_details_dict['FUNCTION'](args_dict)
                domain_id = function_value
                output_dict[field_tag] = (function_value, 'DOMAIN') ##########
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
                output_dict[field_tag] = (None, field_details_dict['config_type'])

    return domain_id


@typechecked
def do_hash_fields(output_dict :dict[str, tuple[any, str]], root_element, root_path, config_name,  
                   config_dict :dict[str, dict[str, str | None]], error_fields_set, 
                   pk_dict :dict[str, any]):
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
                print(f"ERROR: HASH field {field_tag} is missing 'fields' attributes in config:{config_name}")
                logger.error(f"HASH field {field_tag} is missing 'fields' attributes in config:{config_name}")
            for field_name in field_details_dict['fields'] :
                if field_name in output_dict and output_dict[field_name][0] is not None:
                    value_list.append(output_dict[field_name][0])
            ## -->> hash_input =  "|".join(str(value_list))
            hash_input =  "|".join(map(str, value_list))
            hash_value = create_hash(hash_input)
            output_dict[field_tag] = (hash_value, 'HASH')
            # treat as PK and include in that dictionary
            pk_dict[field_tag] = hash_value
            logger.info((f"     HASH (PK) {hash_value} for "
                         f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))

            
@typechecked
def do_priority_fields(output_dict :dict[str, tuple[any, str]], root_element, root_path, config,  
                       config_dict :dict[str, dict[str, str | None]], error_fields_set, 
                       pk_dict :dict[str, any]):
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

        for value_field_pair in sorted_contents:
            if value_field_pair[0] in output_dict and output_dict[value_field_pair[0]][0] is not None:
                output_dict[priority_name] = output_dict[value_field_pair[0]]
                pk_dict[priority_name] = output_dict[value_field_pair[0]][0]
                break

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
def sort_output_dict(output_dict :dict[str, tuple[any, str]], config_dict :dict[str, dict[str, str | None]], config_name):
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
                                 config_dict :dict[str, dict[str, str | None]], error_fields_set, 
                                 pk_dict :dict[str, any]):
    """  Parses for each field in the metadata for a config out of the root_element passed in.
         You may have more than one such root element, each making for a row in the output.

        If the configuration includes a field of config_type DOMAIN, the value it generates
        will be compared to the domain specified in the config in. If they are different, null is returned.
        This is how  OMOP "domain routing" is implemented here.
    """
    output_dict = {} #  :dict[str, tuple]  a record
    domain_id = None
    logger.info((f"  ROOT for config:{config_name}, we have tag:{root_element.tag}"
                 f" attributes:{root_element.attrib}"))

    do_none_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_constant_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_basic_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    do_derived_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    domain_id = do_domain_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_hash_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    priority_field_names = do_priority_fields(output_dict, root_element, root_path, config_name,  config_dict,
                                              error_fields_set, pk_dict)
    
    output_dict = sort_output_dict(output_dict, config_dict, config_name)

    expected_domain_id = config_dict['root']['expected_domain_id']
    if (expected_domain_id == domain_id or domain_id is None):
        return (output_dict, error_fields_set)
    else:
        # reject this data if the datum is intended for a different domain
        return (None, None)


@typechecked
def parse_config_from_file(tree, config_name, 
                           config_dict :dict[str, dict[str, str | None]], filename, 
                           pk_dict :dict[str, any]):
    """ The main logic is here.
        Given a tree from ElementTree representing a CCDA document
        (ClinicalDocument, not just file),
        parse the different domains out of it (1 config each), linking PK and FKs between them.

        Returns a list, output_list, of dictionaries, output_dict, keyed by field name,   :dict[dict, tuple] FIX TODO
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
        force=True, level=logging.WARNING)

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
        logger.error((f"CONFIG couldn't find root element for {config_name}"
                      f" with {config_dict['root']['element']}"))
        return None

    output_list = []
    error_fields_set = set()
    logger.info(f"NUM ROOTS {config_name} {len(root_element_list)}")
    for root_element in root_element_list:
        (output_dict, element_error_set) = parse_config_for_single_root(root_element, root_path, config_name, config_dict, error_fields_set, pk_dict)
        if output_dict is not None:
            output_list.append(output_dict)
        if element_error_set is not None:
            error_fields_set.union(element_error_set)

    # report fields with errors
    if len(error_fields_set) > 0:
        logger.error(f"DOMAIN Fields with errors in config {config_name} {error_fields_set}")

    return output_list


@typechecked
def parse_doc(file_path, metadata :dict[str, dict[str, dict[str, str]]]):
    """ Parses many meta configs from a single file, collects them in omop_dict.
        Returns omop_dict, a  dict keyed by configuration names, 
          each a list of record/row dictionaries.
    """
    omop_dict = {}
    pk_dict = {}
    tree = ET.parse(file_path)
    base_name = os.path.basename(file_path)
    for config_name, config_dict in metadata.items():
        data_dict_list = parse_config_from_file(tree, config_name, config_dict, base_name, pk_dict)
        if config_name in omop_dict: # CHRIS
            omop_dict[config_name] = omop_dict[config_name].extend(data_dict_list)
        else:
            omop_dict[config_name] = data_dict_list
    return omop_dict


@typechecked
def print_omop_structure(omop :dict[str, list[ dict[str, tuple[any, str ] ] ] ], 
                         meta_data :dict[str, dict[str, dict[str, str ] ] ] ):
    
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
                    print(f"\n\nDOMAIN: {domain}")
                    for field, parts in domain_data_dict.items():
                        print(f"    FIELD:{field}")
                        print(f"        parts type {type(parts[0])}")
                        print(f"        parts type {type(parts[1])}")
                        print(f"        VALUE:{parts[0]}")
                        print(f"        PATH:{parts[1]}")
                        print(f"        ORDER: {meta_data[domain][field]['order']}")
                        n = n+1
                    print(f"\n\nDOMAIN: {domain} {n}\n\n")

                    
@typechecked
def process_file(filepath):
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

    meta_data = get_meta_dict()
    omop_data = parse_doc(filepath, meta_data)
    if omop_data is not None or len(omop_data) < 1:
        print_omop_structure(omop_data, meta_data)
    else:
        logger.error(f"FILE no data from {filepath}")


def main() :
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
    args = parser.parse_args()

    if args.filename is not None:
        process_file(args.filename)
    elif args.directory is not None:
        only_files = [f for f in os.listdir(args.directory) if os.path.isfile(os.path.join(args.directory, f))]
        for file in (only_files):
            if file.endswith(".xml"):
            	process_file(os.path.join(args.directory, file))
    else:
        logger.error("Did args parse let us  down? Have neither a file, nor a directory.")


if __name__ == '__main__':
    main()
