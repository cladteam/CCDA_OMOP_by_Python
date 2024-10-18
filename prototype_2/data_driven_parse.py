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

2024-07-31: visit_concept_id is incorrect, It's picking up pnemonia a Condition
               when we want a Visit domain_id
"           need to run on multiple files, need to bring over the test script and correct_files,
"           test script needs sophistication to correlate input with expected output
"""

# import pandas as pd
# mamba install -y -q lxml

import argparse
import datetime
import logging
import os
import re
import sys
import hashlib
import zlib
import ctypes
import traceback
from lxml import etree as ET
from prototype_2.metadata import get_meta_dict
from lxml.etree import XPathEvalError

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
    hash_value = hashlib.md5(input_string.encode('utf-8'))
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


def create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict):
    # creates a tracing dictionary with most elements populated

    # dig out templateId
    parts = config_root_path.split("/")
    template_id = ""
    template_parts = filter(lambda x: re.match(".*template", x), parts)
    for possibility in template_parts:
        template_match = re.match(r"hl7:templateId\[\@root='(.*)']", possibility)
        if template_match:
            template_id = template_match.group(1)

    tracing_dict = {
        'root_xpath': root_xpath,
        'domain': domain,
        'field_tag': field_tag,
        'config_type': field_details_dict['config_type'],
        'config_root_path' : config_root_path,
        'template_id' : template_id
    }
    return tracing_dict


def print_tracing_dict(tracing_dict):
    for key in tracing_dict:
        print(f"TRACE {tracing_dict['domain']} {tracing_dict['field_tag']} {key} {tracing_dict[key]}")
        print("TRACE ============ ")

def parse_field_from_dict(field_details_dict, domain_root_element, domain, field_tag, root_xpath, tree, config_root_path):
    """ Retrieves a value for the field descrbied in field_details_dict that lies below
        the domain_root_element.
        Domain and field_tag are here for error messages.
    """
    tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)

    if 'element' not in field_details_dict:
        logger.error(("FIELD could find key 'element' in the field_details_dict:"
                     f" {field_details_dict} root:{root_xpath}"))
        return (None, tracing_dict)

    logger.info(f"    FIELD {field_details_dict['element']} for {domain}/{field_tag}")
# TODO FIX
    #field_element = domain_root_element.find(field_details_dict['element'], ns)
    field_element = None

    try:
        field_element = domain_root_element.xpath(field_details_dict['element'], namespaces=ns)
    except XPathEvalError as e:
        pass # I know
        #print(f"FAILED on  {field_details_dict['element']}")
    if field_element is None:
        logger.error((f"FIELD could not find field element {field_details_dict['element']}"
                      f" for {domain}/{field_tag} root:{root_xpath} {field_details_dict} "))
        return (None, tracing_dict)

    if 'attribute' not in field_details_dict:
        logger.error((f"FIELD could not find key 'attribute' in the field_details_dict:"
                     f" {field_details_dict} root:{root_xpath}"))
        return (None, tracing_dict)

    logger.info((f"       ATTRIBUTE   {field_details_dict['attribute']} "
                 f"for {domain}/{field_tag} {field_details_dict['element']} "))

    attribute_value = None
## FIX: should this LOOP???!!!!! TODO
    if len(field_element) > 1:
        print(f"parse_field_from_dict SHOULD LOOP!!! there is more than just one!!! {domain} {field_tag}")
    if len(field_element) > 0:


        # Parse for value
        attribute_value = field_element[0].get(field_details_dict['attribute'])
        if field_details_dict['attribute'] == "#text":
            try:
                attribute_value = field_element.text
            except Exception as e:
                print((f"ERROR: no text elemeent for field element {field_element} "
                        f"for {domain}/{field_tag} root:{root_xpath}"))
                logger.error((f"no text elemeent for field element {field_element} "
                        f"for {domain}/{field_tag} root:{root_xpath}"))
        if attribute_value is None:
            logger.warning((f"no value for field element {field_details_dict['element']} "
                        f"for {domain}/{field_tag} root:{root_xpath}"))


        # assemble tracing_dict
        tracing_dict['attribute_value'] =  attribute_value
        tracing_dict['element_xpath'] = re.sub(r'{.*?}', '', tree.getelementpath(field_element[0]))
    else:
        logger.warning((f"no value for field element {field_details_dict['element']} "
                        f"for {domain}/{field_tag} root:{root_xpath}"))

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
                logger.error(f" UNKNOWN DATA TYPE: {field_details_dict['data_type']} {domain} {field_tag}")
            return (attribute_value, tracing_dict)
        else:
            return (None, tracing_dict)
    else:
        return (attribute_value, tracing_dict)


def do_none_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path):
    for (field_tag, field_details_dict) in domain_meta_dict.items():
        logger.info((f"     NONE FIELD domain:'{domain}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag is None:
            tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)
            output_dict[field_tag] = (None, tracing_dict)


def do_constant_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path):
    for (field_tag, field_details_dict) in domain_meta_dict.items():
        logger.info((f"     CONSTANT FIELD domain:'{domain}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag == 'CONSTANT':
            constant_value = field_details_dict['constant_value']
            tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)
            tracing_dict['attribute_value'] =  constant_value
            output_dict[field_tag] = (constant_value, tracing_dict)


def do_basic_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, pk_dict, tree, config_root_path):
    """
    """
    for (field_tag, field_details_dict) in domain_meta_dict.items():
        logger.info((f"     FIELD domain:'{domain}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']

        if config_type_tag == 'FIELD':
            value_dict_pair = parse_field_from_dict(field_details_dict, root_element,
                                                    domain, field_tag, root_xpath, tree, config_root_path)
            print(f"FML1 FIELD {value_dict_pair[0]}")
            output_dict[field_tag] = value_dict_pair
            logger.info(f"     FIELD for {domain}/{field_tag} {value_dict_pair}")

        elif config_type_tag == 'PK':
            logger.info(f"     PK for {domain}/{field_tag}")
            value_dict_pair = parse_field_from_dict(field_details_dict, root_element,
                                                    domain, field_tag, root_xpath, tree, config_root_path)
            output_dict[field_tag] = value_dict_pair
            print(f"FML2 PK {field_tag} {value_dict_pair[0]}")
            pk_dict[field_tag] = value_dict_pair

        elif config_type_tag == 'FK':
            logger.info(f"     FK for {domain}/{field_tag}")
            tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)
            if field_tag in pk_dict:
# TODO link trace of source?
                tracing_dict['attribute_value'] = pk_dict[field_tag][0]
                print(f"FML3all {field_tag} {pk_dict[field_tag]} ")
                print(f"FML3just0 {field_tag} {pk_dict[field_tag][0]} ")
                print("")
                output_dict[field_tag] = (pk_dict[field_tag][0], tracing_dict)
            else:
                logger.error(f"FK could not find {field_tag}  in pk_dict for {domain}/{field_tag}")
                path = root_xpath + "/"
                if 'element' in field_details_dict:
                    path = path + field_details_dict['element'] + "/@"
                else:
                    path = path + "no element/"
                if 'attribute' in field_details_dict:
                    path = path + field_details_dict['attribute']
                else:
                    path = path + "no attribute/"
                # TODO  a better trace_dict
                output_dict[field_tag] = (None, tracing_dict)
                error_fields_set.add(field_tag)


def do_derived_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path):
    # Do derived values now that their inputs should be available in the output_dict
    # Except for a special argument named 'default', when the value is what is other wise the field to look up in the output dict.

    for (field_tag, field_details_dict) in domain_meta_dict.items():
        if field_details_dict['config_type'] == 'DERIVED':
            tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)
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
                        logger.error((f"DERIVED domain:{domain} field:{field_tag} could not "
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
                tracing_dict['attribute_value'] =  function_value
                output_dict[field_tag] = (function_value, tracing_dict)
                logger.info((f"     DERIVED {function_value} for "
                                f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))
            except KeyError as e:
                print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED exception: {e}")
                logger.error(f"DERIVED KeyError {field_tag} function can't find key it expects in {args_dict}")
                # TODO  a better trace_dict
                tracing_dict['attribute_value'] =  constant_value
                output_dict[field_tag] = (None, tracing_dict)
            except TypeError as e:
                print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED exception: {e}")
                logger.error((f"DERIVED TypeError {field_tag} possibly calling something that isn't a function"
                              " or that function was passed a null value."
                              f" {field_details_dict['FUNCTION']}. You may have quotes "
                              "around it in  a python mapping structure if this is a "
                              f"string: {type(field_details_dict['FUNCTION'])}"))
                # TODO  a better trace_dict
                output_dict[field_tag] = (None, tracing_dict)
                print(f"DER3: {output_dict[field_tag]}")


def do_domain_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path):
    # nearly the same as derived above, but returns the domain for later filtering

    domain_id = None
    for (field_tag, field_details_dict) in domain_meta_dict.items():
        if field_details_dict['config_type'] == 'DOMAIN':
            tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)
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
                        logger.error((f"DERIVED domain:{domain} field:{field_tag} could not "
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
                tracing_dict['attribute_value'] =  function_value
                output_dict[field_tag] = (function_value, tracing_dict)
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
                tracing_dict['attribute_value'] = function_value
                output_dict[field_tag] = (None, tracing_dict)

    return domain_id


def do_hash_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, pk_dict, config_root_path):
    """ These are basically derived, but the argument is a lsit of field names, instead of
        a fixed number of individually named fields.
        Dubiously useful in an environment where IDs are  32 bit integers.
        See the code above for converting according to the data_type attribute
        where a different kind of hash is beat into an integer.
    """

    for (field_tag, field_details_dict) in domain_meta_dict.items():
        if field_details_dict['config_type'] == 'HASH':
            tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)
            value_list = []
            for field_name in field_details_dict['fields'] :
                if field_name in output_dict and output_dict[field_name] is not None:
                    value_list.append(output_dict[field_name])
            ## -->> hash_input =  "|".join(str(value_list))
            hash_input =  "|".join(map(str, value_list))
            hash_value = create_hash(hash_input)
            tracing_dict['attribute_value'] = hash_value
            output_dict[field_tag] = (hash_value, tracing_dict)

            # treat as PK and include in that dictionary
            pk_dict[field_tag] = (hash_value, tracing_dict)
            logger.info((f"     HASH (PK) {hash_value} for "
                         f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))


def do_priority_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, pk_dict, config_root_path):
    """
        Returns the list of  priority_names so the chosen one (first non-null) can be
        added to output fields Also, adds this field to the PK list?

        Within the domain_meta_dict, find all fields tagged with priority and group
        them by their priority names in a dictionary keyed by that name
        Ex. { 'person_id': [ ('person_id_ssn', 1), ('person_id_unknown', 2) ]
        Sort them, choose the first one that is not None.

 AFU    AFU  NB now there is a separate config_type PRIORITY to compliment the priority attribute.
        So you might have person_id_npi, person_id_ssn and person_id_hash tagged with priority
        attributes to create a field person_id, but then also another field, just plain person_id.
        The point of it is to have a unique place to put that field's order attribute. The code
        here (and in the ordering code later) must be aware of a  that field in the
        domain_meta_dict (where it isn't used) ...and not clobber it. It's an issue over in the
        sorting/ordering.
    """

    # Create Ref Data
    # for each new field, create a list of source fields and their priority:
    # Ex. [('person_id_other', 2), ('person_id_ssn', 1)]
    priority_fields = {}
    for field_tag, field_details_dict in domain_meta_dict.items():
        tracing_dict = create_base_tracing_dict(config_root_path, root_xpath, domain, field_tag, field_details_dict)
        if  'priority' in field_details_dict:
            new_field_name = field_details_dict['priority'][0]
            priority_number = field_details_dict['priority'][1]
            if new_field_name in priority_fields:
                priority_fields[new_field_name].append( (field_tag, field_details_dict['priority'][1]))
            else:
                priority_fields[new_field_name] = [ (field_tag, field_details_dict['priority'][1]) ]

    # Choose Fields
    # first field in each set with a non-null value in the output_dict adds that value to the dict with it's priority_name
    sorted_contents=None
    for priority_name, priority_contents in priority_fields.items():
        sorted_contents = sorted(priority_contents, key=lambda x: x[1])
        # Ex. [('person_id_ssn', 1), ('person_id_other, 2)]

        for value_field_pair in sorted_contents:
            # look at the value, not the (value, tracing_dict) pair for Noneness. It needs that last [0]
            if value_field_pair[0] in output_dict and output_dict[value_field_pair[0]][0] is not None:
                tracing_dict['attribute_value'] = output_dict[value_field_pair[0]]
                output_dict[priority_name] = output_dict[value_field_pair[0]]
#  TODO TRACING
                print(f"XXXXX priority is returning pr_name:{priority_name} field_name:{value_field_pair[0]} value:{output_dict[value_field_pair[0]][0]}")
                pk_dict[priority_name] = (output_dict[value_field_pair[0]][0], 'PRIORITY')
                break

    if sorted_contents:
        print(f"XXXXXXXXX PRIORITY DID NOT FIND ONE {domain} {sorted_contents}")
    else:
        print(f"XXXXXXXXX PRIORITY DID NOT FIND ONE {domain}")
    return priority_fields


def get_extract_order_fn(dict):
    def get_order_from_dict(field_key):
        if 'order' in dict[field_key]:
            logger.info(f"{field_key} {dict[field_key]['order']}")
            return int(dict[field_key]['order'])
        else:
            logger.info(f"extract_order_fn, no order in {field_key}")
            return int(sys.maxsize)

    return get_order_from_dict

def get_filter_fn(dict):
    def has_order_attribute(key):
        return 'order' in dict[key] and dict[key]['order'] is not None
    return has_order_attribute

def sort_output_dict(output_dict, domain_meta_dict, domain):
    """ Sorts the ouput_dict by the value of the 'order' fields in the associated
        domain_meta_dict. Fields without a value, or without an entry used to
        come last, now are omitted.
    """
    ordered_output_dict = {}

    sort_function = get_extract_order_fn(domain_meta_dict) # curry in the domain arg.
    ordered_keys = sorted(domain_meta_dict.keys(), key=sort_function)

    filter_function = get_filter_fn(domain_meta_dict)
    filtered_ordered_keys = filter(filter_function, ordered_keys)

    for key in filtered_ordered_keys:
        if key in output_dict:
            ordered_output_dict[key] = output_dict[key]

    return ordered_output_dict


def parse_domain_for_single_root(root_element, root_xpath, domain, domain_meta_dict, error_fields_set, pk_dict, tree, config_root_path):
    """  Parses for each field in the metadata for a domain out of the root_element passed in.
         You may have more than one such root element, each making for a row in the output.

        If the configuration includes a field of config_type DOMAIN, the value it generates
        will be compared to the domain passed in. If they are different, null is returned.
        This is how  OMOP "domain routing" is implemented here.

        Args:
            - root_element: an XML element from
            - root_xpath: a path queried out of the EleementTree package. It incudes instance numbers that the root_path in the config metatdata does not.
            - domain: the name of the domain, utlimately used in do_domain_fields to filter this data in or out
            - domain_meta_dict: the meatadata for this domain, what is in e.g. prototype_2/metadata/measurement.py
            - error_fields_set: a list on output for fields that had issues
            - pk_dict: a dictionary of values of primary keys, e.g. the person_id of the patient previously parsed from the header
            - tree: the current file in parsed form from the ElementTree's parsing functions (xpath)
            - config_root_path:
    """
    output_dict = {}
    domain_id = None
    logger.info((f"  ROOT for domain:{domain}, we have tag:{root_element.tag}"
                 f" attributes:{root_element.attrib}"))

    # NONE, CONSTANT
    # These two don't use data in the file.
    do_none_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path)
    do_constant_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path)

    # FIELD, PK and FK
    # This one is the only one that parses data out of the file (FIELD and PK). It is the only one that calls parse_field_from_dict()
    do_basic_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, pk_dict, tree, config_root_path)

    # These combine or select from what has been parsed.
    # DERIVED
    do_derived_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path)
    # DOMAIN
    domain_id = do_domain_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, config_root_path)
    # HASH
    do_hash_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, pk_dict, config_root_path)
    # PRIORITY
    priority_field_names = do_priority_fields(output_dict, root_element, root_xpath, domain,  domain_meta_dict, error_fields_set, pk_dict, config_root_path)

    output_dict = sort_output_dict(output_dict, domain_meta_dict, domain)

    if (domain == domain_id or domain_id is None):
        return (output_dict, error_fields_set)
    else:
        return (None, None)


def parse_domain_from_dict(tree, domain, domain_meta_dict, filename, pk_dict):
    """ The main logic is here.
        Given a tree from ElementTree representing a CCDA document
        (ClinicalDocument, not just file),
        parse the different domains out of it, linking PK and FKs between them.
        Returns a list, output_list, of dictionaries, output_dict, keyed by field name,
        containing a list of the value and the path to it:
            [ { field_1 : (value, path), field_2: (value, path)},
              { field_1: (value, path)}, {field_2: (value, path)} ]
        It's a list of because you might have more than one instance of the root path, like when you
        get many observations.
    """

    # log to a file per file/domain
    base_name = os.path.basename(filename)
    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        filename=f"logs/log_domain_{base_name}_{domain}.log",
        force=True, level=logging.WARNING)

    # Find root
    if 'root' not in domain_meta_dict:
        logger.error(f"DOMAIN {domain} lacks a root element.")
        return None

    if 'element' not in domain_meta_dict['root']:
        logger.error(f"DOMAIN {domain} root lacks an 'element' key.")
        return None

    config_root_path = domain_meta_dict['root']['element']
    logger.info((f"DOMAIN >>  domain:{domain} root:{domain_meta_dict['root']['element']}"
                 f"   ROOT path:{config_root_path}"))
    #root_element_list = tree.findall(domain_meta_dict['root']['element'], ns)
    root_element_list = tree.xpath(domain_meta_dict['root']['element'], namespaces=ns)
    if root_element_list is None or len(root_element_list) == 0:
        logger.error((f"DOMAIN couldn't find root element for {domain}"
                      f" with {domain_meta_dict['root']['element']}"))
        return None

    output_list = []
    error_fields_set = set()
    logger.info(f"NUM ROOTS {domain} {len(root_element_list)}")
    for root_element in root_element_list:
        root_xpath = re.sub(r'{.*?}', '', tree.getelementpath(root_element))
        root_xpath = re.sub(r'HL7:', '', root_xpath)
        (output_dict, element_error_set) = parse_domain_for_single_root(root_element, root_xpath, domain, domain_meta_dict, error_fields_set, pk_dict, tree, config_root_path)
        if output_dict is not None:
            output_list.append(output_dict)
        if element_error_set is not None:
            error_fields_set.union(element_error_set)

    # report fields with errors
    if len(error_fields_set) > 0:
        logger.error(f"DOMAIN Fields with errors in domain {domain} {error_fields_set}")

    return output_list


def parse_doc(file_path, metadata):
    """ Parses many domains from a single file, collects them
        into a dict to return.

        The omop_dict is keyed by domain and has data dicts as values.
        The data_dicts are keyed by field and have (value, tracing_dict) pairs for values.
        The print_omop_structure() function shows this too.
    """
    omop_dict = {}
    pk_dict = {}
    tree = ET.parse(file_path)
    base_name = os.path.basename(file_path)
    for domain, domain_meta_dict in metadata.items():
        domain_data_dict = parse_domain_from_dict(tree, domain, domain_meta_dict, base_name, pk_dict)
        omop_dict[domain] = domain_data_dict
    return omop_dict


def print_omop_structure(omop, meta_data):
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
                    for field_key in domain_data_dict:
                        print(f"    FIELD:{field_key}")
                        print(f"        VALUE:{domain_data_dict[field_key][0]}")
                        #print(f"        TYPE/TRACE:{domain_data_dict[field_key][1]}")
                        print(f"        ORDER: {meta_data[domain][field_key]['order']}")
                        n = n+1
                    print(f"\n\nDOMAIN: {domain} {n}\n\n")


def process_file(filepath):
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


    if False:  # for getting them on the Foundry
        from foundry.transforms import Dataset
        ccd_ambulatory = Dataset.get("ccda_ccd_b1_ambulatory_v2")
        ccd_ambulatory_files = ccd_ambulatory.files().download()
        ccd_ambulatory_path = ccd_ambulatory_files['CCDA_CCD_b1_Ambulatory_v2.xml']



if __name__ == '__main__':
    main()
