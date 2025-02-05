import re
#import prototype_2.metadata
from prototype_2.metadata.test import metadata

"""
This script aims to link the various field types from XML source
to OMOP table/field destination. In some cases it's direct
XML to OMOP. In others there are intervening steps of calculation
or mapping. The different field types are processed in an order
that is reflected here.
field types:  None, CONSTANT, FIELD, DERIVED, HASH, PRIORITY.

Key to this whole world is a hierarchy of config_types and what they can use:
- CONSTANT, FIELD, PK are at the bottom and don't/can't depend on anything
- DERIVED and DOMAIN (latter ignored here) can use as input either of the previous three
- HASH can use either of the previous two categories
- PRIORITY can use eithe of the previous three categories (TBC here)

RESET: clearer  data_dict

    direct fields like PK,  FIELD, CONSTANT:
       config_key -->  field_key --> 
           { 'type': 'constant',
             'arg' : path | constant | PK field key ,
             'order' : n
           }
        
    derived fields like FK
       config_key -->  field_key --> 
           { 'type': 'FK', 
             'values-dict' : { key? : field_name_1 }
             'order' : n
           } 

    derived fields like DERIVED
       config_key -->  field_key --> 
           { 'type': function-name, 
             'values-dict' : { arg_name_1: struct_1, ...arg_name_n: struct_n }
             'args-dict' : { arg_name_1: field_name_1, ...field_name_n: struct_n }
             'order' : n
           } 

    hash  fields like HASH
       config_key -->  field_key --> 
           { 'type': 'hash', 
             'values-dict' : { arg_name_1: struct_1, ...arg_name_n: struct_n }
             'order' : n
           }

    priority fields like PRIORITY
       config_key -->  field_key --> 
           { 'type': 'priority', 
             'values-list' :  arg_name_1: struct_1, ...arg_name_n: struct_n }
             'args-list' :  arg_name_1: field_name_1, ...field_name_n: struct_n }
             'order' : n
            }
"""

print_order_flag = False
print_derived_to_base = True


def strip_detail(input_string):
    """ strips namespaces and conditionals out of an XPath
    """
    interim =  re.sub(r'hl7:', '', input_string)
    interim = re.sub(r'\[.*\]', '', interim)
    return interim


def get_base_elements(metadata):
    """
    Fetches keys of elements that are fetched from XML.
    The types of these keys are FIELD, PK.
    The keys are returned as config, field pairs.

       config_key -->  field_key --> 
           { 'type': 'constant',
             'arg' : path | constant | FK field key ,
             'order' : n
    """

    base_field_dict = {}
    for config_key in metadata:
        base_field_dict[config_key] = {}
        root_path = metadata[config_key]['root']['element']
        root_path = strip_detail(root_path)

        for field_key in metadata[config_key]:
            base_field_dict[config_key][field_key] = {}

            # type
            base_field_dict[config_key][field_key]['type']='constant'

            # arg
            if metadata[config_key][field_key]['config_type'] is None:
                base_field_dict[config_key][field_key]['arg'] = 'None'

            if metadata[config_key][field_key]['config_type'] == 'CONSTANT':
                base_field_dict[config_key][field_key]['arg'] = metadata[config_key][field_key]['constant_value']


            if metadata[config_key][field_key]['config_type'] in ('FIELD', 'PK'):
                path=(f"{root_path}/"
                      f"{strip_detail(metadata[config_key][field_key]['element'])}"
                      f"@{strip_detail(metadata[config_key][field_key]['attribute'])}")
                base_field_dict[config_key][field_key]['arg'] = path


            # order
            if 'order' in metadata[config_key][field_key]:
                base_field_dict[config_key][field_key]['order'] = metadata[config_key][field_key]['order']
            else:
                base_field_dict[config_key][field_key]['order'] = None
          

    return base_field_dict


def get_derived_fields(metadata):
    """
    Fetches functions and arguments (field names) of elements that are 
    derived from base fields.
    The types of these keys are DERIVED and DOMAIN. Only DERIVED are fetched
    here because DOMAIN are part of denying fields that are in the wrong domain. TODO.

    Returns for each config_key, field_key a dictionary with 'function' and 'args' 
    keys and associated values.

     config_key --> field_key --> {'type': function name,
                                   'args' : [ field names ],
                                   'order' : int }
       # DERIVED 
       config_key -->  field_key --> 
           { 'type': function-name, 
             'args-dict' : { arg_name_1: struct_1, ...arg_name_n: struct_n }
             'order' : n
           }
       # FK
       config_key -->  field_key --> 
           { 'type': 'FK', 
             'args-dict' : { FK_field_key : struct_1 }
             'order' : n
           } 
    """

    derived_field_dict = {}
        
    for config_key in metadata:
        derived_field_dict[config_key] = {}
        for field_key in metadata[config_key]:
            if metadata[config_key][field_key]['config_type'] == 'FK':
                derived_field_dict[config_key][field_key] = {}

                # type
                derived_field_dict[config_key][field_key]['type'] = 'FK'
    
                # values-dict only
                try:
                    ####fk_field_key = metadata[config_key][field_key]['config_type']['FK']
                    fk_field_key = metadata[config_key][field_key]['FK']
                except Exception as x:
                    print(f"ERROR: {config_key} {field_key} ")
                    print(f"   {x}")
                    raise x
                derived_field_dict[config_key][field_key]['values-dict'] =  { 
                            fk_field_key:  metadata[config_key][fk_field_key]
                }

                #order
                if 'order' in metadata[config_key][field_key]:
                    derived_field_dict[config_key][field_key]['order'] = metadata[config_key][field_key]['order']
                else:
                    derived_field_dict[config_key][field_key]['order'] = None

            if metadata[config_key][field_key]['config_type'] == 'DERIVED':
                derived_field_dict[config_key][field_key] = {}

                # type
                derived_field_dict[config_key][field_key]['type'] = getattr(metadata[config_key][field_key]['FUNCTION'], "__name__") +"()"

                # args & values
                arg_keys= metadata[config_key][field_key]['argument_names'].keys()
                args_hash = {}
                values_hash = {}
                for arg_key in arg_keys:
                    # Use the arg name to get the field_key it refers to. Does assume the  references are within the same config_key.
                    arg_field_key = metadata[config_key][field_key]['argument_names'][arg_key]
                    if arg_key == 'default':
                        values_hash[arg_key] = arg_field_key 
                        args_hash[arg_key] = '(constant default)'
                    else:
                        args_hash[arg_key] = arg_field_key
                        if metadata[config_key][arg_field_key]['config_type'] == 'FIELD': 
                            #values_hash[arg_key] =  metadata[config_key][arg_field_key]
                            root_path =  metadata[config_key]['root']['element'] 
                            values_hash[arg_key] =  (f"{root_path}/"
                                                   f"{metadata[config_key][arg_field_key]['element']}"
                                                   "/@"
                                                   f"{metadata[config_key][arg_field_key]['attribute']}")
                        elif metadata[config_key][arg_field_key]['config_type'] == 'CONSTANT': 
                            values_hash[arg_key] =  metadata[config_key][arg_field_key]['constant_value']
                        else:
                            values_hash[arg_key] =  metadata[config_key][arg_field_key]
                derived_field_dict[config_key][field_key]['args-dict'] = args_hash
                derived_field_dict[config_key][field_key]['values-dict'] = values_hash

                # order
                if 'order' in metadata[config_key][field_key]:
                    derived_field_dict[config_key][field_key]['order'] = metadata[config_key][field_key]['order']
                else:
                    derived_field_dict[config_key][field_key]['order'] = None


    return derived_field_dict


def get_hash_fields(metadata, derived_field_dict):
    """
    	'measurement_id_hash': {
    	    'config_type': 'HASH',
            'fields' : [ 'measurement_id_root', 'measurement_id_extension' ],

       config_key -->  field_key --> 
           { 'function': 'hash', 
             'values-dict' : { arg_name_1: struct_1, ...arg_name_n: struct_n }
             'order' : n
    """
    hash_field_dict = {}
        
    for config_key in metadata:
        hash_field_dict[config_key] = {}
        for field_key in metadata[config_key]:
            if metadata[config_key][field_key]['config_type'] == 'HASH':
                hash_field_dict[config_key][field_key] = {}
    
                # type 
                hash_field_dict[config_key][field_key]['type'] = 'hash'

                # values-dict only
                arg_fields = metadata[config_key][field_key]['fields']

                if False:
                    # doesn't pull up deeper definitions
                    hash_field_dict[config_key][field_key]['values-dict'] = {}
                    for arg_key in arg_fields:
                        hash_field_dict[config_key][field_key]['values-dict'][arg_key] = metadata[config_key][arg_key]

                else:
                    values_hash = {}
                    for arg_key in arg_fields:
                        if metadata[config_key][arg_key]['config_type'] == 'DERIVED': 
                            if False:
                                # doesn't pull up deeper definitions
                                values_hash[arg_key] =  metadata[config_key][arg_key]
                            else:
                                if False:
                                    # madness, deeper, but raw
                                    values_hash[arg_key] =  derived_field_dict[config_key][arg_key]
                                    #arg:test_derived_field value:{'type': 'map_hl7_to_omop_concept_id()', 
                                    #                              'args-dict': {'concept_code': 'field_code', 'vocabulary_oid': 'field_oid', 'default': '(constant default)'}, 
                                    #                              'values-dict': {'concept_code': 'fake/doc/path/id/@code', 'vocabulary_oid': 'fake/doc/path/id/@codeSystem', 'default': 0}, 
                                    #                              'order': 6}
                                else:
                                    values_list  = []
                                    for (k,v) in derived_field_dict[config_key][arg_key]['values-dict'].items():
                                        values_list.append(v)
                                    values_hash[arg_key] =  f" {derived_field_dict[config_key][arg_key]['type']} {values_list} "

                        elif metadata[config_key][arg_key]['config_type'] == 'FIELD': 
                            if False:
                                # just the dict of config data
                                values_hash[arg_key] =  metadata[config_key][arg_key]
                            else:
                                root_path =  metadata[config_key]['root']['element'] 
                                values_hash[arg_key] =  (f"{root_path}/"
                                                         f"{metadata[config_key][arg_key]['element']}"
                                                         "/@"
                                                         f"{metadata[config_key][arg_key]['attribute']}")
                        elif metadata[config_key][arg_key]['config_type'] == 'CONSTANT': 
                             values_hash[arg_key] =  metadata[config_key][arg_key]['constant_value']
                        else:
                            values_hash[arg_key] =  metadata[config_key][arg_key]
                    hash_field_dict[config_key][field_key]['values-dict'] = values_hash


                # order
                if 'order' in metadata[config_key][field_key]:
                    hash_field_dict[config_key][field_key]['order'] = metadata[config_key][field_key]['order']
                else:
                    hash_field_dict[config_key][field_key]['order'] = None
    return hash_field_dict


def print_data_hash(data_hash):
    for config_key in sorted(data_hash):
        for field_key in sorted(data_hash[config_key]):
            thing =  data_hash[config_key][field_key]
            #if 'order' in thing and thing['order']:         
            if True:
                print(f"{config_key}/{field_key} type:{thing['type']} order:{thing['order']}")
                if 'arg' in thing:
                    print(f"  arg: \"{thing['arg']}\"")
                else:
                    if 'values-dict' in thing:
                        for arg in thing['values-dict']:
                            if 'args-dict' in thing:
                                # keys  should be parallel
                                print(f"  arg:{arg} name:{thing['args-dict'][arg]} value:{thing['values-dict'][arg]}")
                            else:
                                print(f"  arg:{arg} value:{thing['values-dict'][arg]}")


def merge_second_level_dict(dest_dict, additional_dict):    
    for key in additional_dict:
        if key in dest_dict:
            dest_dict[key] = dest_dict[key] | additional_dict[key]
        else:
            dest_dict[key] = additional_dict[key]
    

def main():
    # just use the imported metadata from test. Don't need this:
    #metadata = prototype_2.metadata.get_meta_dict()
      # config_key --> field_key --> dict (described in data_driven_parse.py)


    # FIELD, PK
    base_field_dict = get_base_elements(metadata)
      # (old) config_key --> field_key --> XML Path
      # (new) config_key --> field_key --> { 'path': XML Path,
      #                                      'order' : int }
    #print_data_hash(base_field_dict)
   
    # DERIVED 
    derived_field_dict = get_derived_fields(metadata) 
      # config_key --> field_key --> {'function': function name,
      #                               'args' : [ field names ],
      #                               'order' : int }
    #print_data_hash(derived_field_dict)

    # HASH
    hash_field_dict = get_hash_fields(metadata, derived_field_dict)
      # config_key --> field_key --> { 'function' : 'hash()',
      #                                'args' : [ field names ],
      #                                'order' : int }
    #print_data_hash(hash_field_dict)
    


    # LINK HASHED to HASHED????????
    # any hashes that use hashes? YES *** To Do **** might even be a bug in data_driven_parse.py !!!


    # SHOW ALL
    if True:
        merged_dict = {}
        merge_second_level_dict(merged_dict, base_field_dict)
        merge_second_level_dict(merged_dict, derived_field_dict)
        merge_second_level_dict(merged_dict, hash_field_dict)
        print_data_hash(merged_dict)



if __name__ == '__main__':
    main()        
