import re
import prototype_2.metadata

"""
This script aims to link the various field types from XML source
to OMOP table/field destination. In some cases it's direct
XML to OMOP. In others there are intervening steps of calculation
or mapping. The different field types are processed in an order
that is reflected here.
field types:  None, CONSTANT, FIELD, DERIVED, HASH, PRIORITY.

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
             'args-hash' : { key? : field_name_1 }
             'order' : n
           } 

    derived fields like DERIVED
       config_key -->  field_key --> 
           { 'type': function-name, 
             'values-hash' : { arg_name_1: struct_1, ...arg_name_n: struct_n }
             'args-hash' : { arg_name_1: field_name_1, ...field_name_n: struct_n }
             'order' : n
           } 

    hash  fields like HASH
       config_key -->  field_key --> 
           { 'function': 'hash', 
             'values-list' : [ arg_name_1: struct_1, ...arg_name_n: struct_n ]
             'args-list' : [ field_name_1: struct_1, ...field_name_n: struct_n ]
             'order' : n
           }

    priority fields like PRIORITY
       config_key -->  field_key --> 
           { 'function': 'priority', 
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

    # (old) config_key --> field_key --> XML Path
    # (new) config_key --> field_key --> { 'path': XML Path,
    #                                      'order' : int }
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

    	    'FUNCTION': VT.map_hl7_to_omop_concept_id,
    	    'argument_names': {
    		    'concept_code': 'measurement_concept_code',
    		    'vocabulary_oid': 'measurement_concept_codeSystem',
                'default': 0
    	    },

    # config_key --> field_key --> {'function': function name,
    #                               'args' : [ field names ],
    #                               'order' : int }
       # DERIVED 
       config_key -->  field_key --> 
           { 'type': function-name, 
             'args-hash' : { arg_name_1: struct_1, ...arg_name_n: struct_n }
             'order' : n
           }
       # FK
       config_key -->  field_key --> 
           { 'type': 'FK', 
             'args-hash' : { FK_field_key : struct_1 }
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
    
                # args-hash
                fk_field_key = metadata[config_key][field_key]['config_type']['FK']
                derived_field_dict[config_key][field_key]['args-hash'] =  { 
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

                # args
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
                derived_field_dict[config_key][field_key]['args-hash'] = args_hash
                derived_field_dict[config_key][field_key]['values-hash'] = values_hash

                # order
                if 'order' in metadata[config_key][field_key]:
                    derived_field_dict[config_key][field_key]['order'] = metadata[config_key][field_key]['order']
                else:
                    derived_field_dict[config_key][field_key]['order'] = None


    return derived_field_dict


def link_derived_to_base(derived_field_dict, base_field_dict):
    # Assumes args, base fields,  for a derived field are in the same
    # config as the derived field.
    linked_field_dict = {}
    for der_config_key in derived_field_dict:
        linked_field_dict[der_config_key] = {}
        for der_field_key in derived_field_dict[der_config_key]:
            linked_field_dict[der_config_key][der_field_key] = {}
            linked_field_dict[der_config_key][der_field_key]['function'] = \
                derived_field_dict[der_config_key][der_field_key]['function']
            linked_field_dict[der_config_key][der_field_key]['order'] = \
                derived_field_dict[der_config_key][der_field_key]['order']

            linked_field_dict[der_config_key][der_field_key]['args'] = []
            for arg in derived_field_dict[der_config_key][der_field_key]['args']:
               xml_path = base_field_dict[der_config_key][arg]['path']
               linked_field_dict[der_config_key][der_field_key]['args'].append(xml_path)

    return linked_field_dict


def find_hash_fields(metadata):
    """

    	'measurement_id_hash': {
    	    'config_type': 'HASH',
            'fields' : [ 'measurement_id_root', 'measurement_id_extension' ],

       config_key -->  field_key --> 
           { 'function': 'hash', 
             'args-list' : [ arg_name_1: struct_1, ...arg_name_n: struct_n ]
             'order' : n
    """
    hash_field_dict = {}
        
    for config_key in metadata:
        hash_field_dict[config_key] = {}
        for field_key in metadata[config_key]:
            if metadata[config_key][field_key]['config_type'] == 'HASH':
                hash_field_dict[config_key][field_key] = {}

                fields = metadata[config_key][field_key]['fields']
                # 'args' in the metadata, called 'fields' here to be more 
                # uniform with DERIVED functions TODO
                hash_field_dict[config_key][field_key]['args'] = fields

                if 'order' in metadata[config_key][field_key]:
                    hash_field_dict[config_key][field_key]['order'] = metadata[config_key][field_key]['order']
                else:
                    hash_field_dict[config_key][field_key]['order'] = None
    return hash_field_dict



def link_hash_to_base(hash_field_dict, base_field_dict, metadata):
    """
    Assumes args, base fields,  for a derived field are in the same
    config as the derived field.

    INPUT: base_field_dict
    # config_key --> field_key --> { 'path': XML Path, 'order' : int }

    UPDATE: hash_field_dict
    # config_key --> field_key --> { 'function' : 'hash()', 'args' : [ field names ], 'order' : int }

    """
    linked_field_dict = {}
    for hash_config_key in hash_field_dict:
        linked_field_dict[hash_config_key] = {}
        for hash_field_key in hash_field_dict[hash_config_key]:
            linked_field_dict[hash_config_key][hash_field_key] = {}

            # Function
            linked_field_dict[hash_config_key][hash_field_key]['function'] = 'hash()'

            # Order/Output
            linked_field_dict[hash_config_key][hash_field_key]['order'] = \
                hash_field_dict[hash_config_key][hash_field_key]['order']

            # Args
            linked_field_dict[hash_config_key][hash_field_key]['args'] = []
            for arg in hash_field_dict[hash_config_key][hash_field_key]['args']:
                if arg in base_field_dict[hash_config_key]:
                    xml_path = 'n/a'
                    if 'path' not in base_field_dict[hash_config_key][arg]:
                        if print_derived_to_base:
                            print((f"link_hash_to_base() INFO field: {hash_config_key}/{hash_field_key} {arg} has NO 'path', "
                                   f"arg:{metadata[hash_config_key][arg]} probably a "
                                   "derived field used by a hash, not a base field"))
                    else:
                        #xml_path = base_field_dict[hash_config_key][hash_field_key]['path']  
                        xml_path = base_field_dict[hash_config_key][arg]['path']  
                        print((f"link_hash_to_base() INFO {hash_config_key}/{hash_field_key} {arg} has 'path', "
                               f"field: {metadata[hash_config_key][hash_field_key]['config_type']} "
                               f"arg: {metadata[hash_config_key][arg]['config_type']} "))
                        # only add an entry when we get here, not above
                        linked_field_dict[hash_config_key][hash_field_key]['args'].append(xml_path)
                elif print_derived_to_base:
                    print((f"link_hash_to_base() INFO {hash_config_key}/{hash_field_key} "
                           f"arg:{arg} (not a base FIELD?) "
                           f"{metadata[hash_config_key][hash_field_key]['config_type']}"))

    return linked_field_dict


def link_hash_to_derived(hash_field_dict, derived_field_dict, metadata):
    """
        if a hash refers to a derived field, this links them.
        In the real data, I haven't seen this.

    INPUT: hash_field_dict
    # config_key --> field_key --> { 'function' : 'hash()', 'args' : [ field names ], 'order' : int }

    INPUT: derived_field_dict
    # config_key --> field_key --> {'function': f. name, 'args' : [ field names ], 'order' : int }

    UPDATE: hash_field_dict
    # config_key --> field_key --> { 'function': 'hash()',
    #                                 'args' : [ args ], ##  these are DERIVED entries, dictionaries
    #                                    [   { 'function': function_name, 'args' : [ XML Paths ]}  ]
    #                                 'order' : int }
    """
    linked_field_dict = {}
    for hash_config_key in hash_field_dict:
        linked_field_dict[hash_config_key] = {}
        for hash_field_key in hash_field_dict[hash_config_key]:
            linked_field_dict[hash_config_key][hash_field_key] = {}

            # Function
            linked_field_dict[hash_config_key][hash_field_key]['function'] = 'hash()'

            # Order/Output
            linked_field_dict[hash_config_key][hash_field_key]['order'] = \
                hash_field_dict[hash_config_key][hash_field_key]['order']
            # Path 
            for arg in hash_field_dict[hash_config_key][hash_field_key]['args']:
                if arg in derived_field_dict[hash_config_key]:
                    linked_field_dict[hash_config_key][hash_field_key]['path'] = derived_field_dict[hash_config_key]

            if hash_field_key in derived_field_dict[hash_config_key]:
                if 'order' in derived_field_dict[hash_config_key][hash_field_key]:
                    linked_field_dict[hash_config_key][hash_field_key]['order'] = \
                        derived_field_dict[hash_config_key][hash_field_key]['order']
            print(f"OUT {hash_config_key}/{hash_field_key} {linked_field_dict[hash_config_key][hash_field_key]}\n")

    return linked_field_dict


def print_data_hash(data_hash):
    for config_key in sorted(data_hash):
        for field_key in sorted(data_hash[config_key]):
            thing =  data_hash[config_key][field_key]
            #if 'order' in thing and thing['order']:         
            if True:
                print(f"{config_key}/{field_key} type:{thing['type']} order:{thing['order']}")
                if 'arg' in thing:
                    print(f"  arg: \"{thing['arg']}\"")
                elif 'args-hash' in thing:
                    for arg in thing['values-hash']:
                        if 'values-hash' in thing:
                            # keys  should be parallel
                            print(f"  arg:{arg} name:{thing['args-hash'][arg]} value:{thing['values-hash'][arg]}")
                        else:
                            print(f"  arg:{arg} name:{thing['args-hash'][arg]}")

def print_data_hash_xxxk(data_hash):
    for config_key in sorted(data_hash):
        for field_key in sorted(data_hash[config_key]):
            if 'order' in data_hash[config_key][field_key] and data_hash[config_key][field_key]['order']:         
                print("")
                for thing_key in data_hash[config_key][field_key]:
                    if isinstance(data_hash[config_key][field_key], list):
                        for x in data_hash[config_key][field_key]:
                           print(f"{config_key}/{field_key} {thing_key} {x}")
                    elif isinstance(data_hash[config_key][field_key], dict):
                        my_object = data_hash[config_key][field_key][thing_key]
                        if isinstance(my_object, list):
                           for sub_object in my_object:
                               print(f"{config_key}/{field_key} {thing_key} {sub_object}")
                        elif thing_key != 'order':
                            print(f"{config_key}/{field_key} {thing_key} {my_object}")
                        elif print_order_flag:
                            print(f"{config_key}/{field_key} {thing_key} {my_object}")
            else:
                if 'order' in data_hash[config_key][field_key]:
                    if print_order_flag: 
                        # can only be None here
                        print(f"  Not output: {config_key} {field_key} {data_hash[config_key][field_key]}")         
                else :
                    if print_order_flag: 
                        print(f"  No order: {config_key} {field_key} {data_hash[config_key][field_key]}")         
            print(f"    data_hash: {config_key}/{field_key}  {data_hash[config_key][field_key]}")
            print("    ------------------------------")
    

        print("")


def merge_second_level_dict(dest_dict, additional_dict):    
    for key in additional_dict:
        if key in dest_dict:
            dest_dict[key] = dest_dict[key] | additional_dict[key]
        else:
            dest_dict[key] = additional_dict[key]
    

def main():
    metadata = prototype_2.metadata.get_meta_dict()
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

    # LINK DERIVED
    ##derived_linked_to_base = link_derived_to_base(derived_field_dict, base_field_dict)
      # config_key --> derived_field_key --> { 'function': function_name,
      #                                         'args' : [ XML Paths ], 
      #                                         'order' : int }

    # HASH
    ##hash_field_dict = find_hash_fields(metadata)
      # config_key --> field_key --> { 'function' : 'hash()',
      #                                'args' : [ field names ],
      #                                'order' : int }
    
    # LINK HASHED to BASE
    ##hash_linked_to_base = link_hash_to_base(hash_field_dict, base_field_dict, metadata)
      # config_key --> field_key --> { 'function': 'hash()',
      #                                 'args' : [ args ], ## these are path strngs
      #                                 'order' : int }


    # LINK HASHED to DERIVED
    ##hash_linked_to_derived = link_hash_to_derived(hash_field_dict, derived_field_dict, metadata)
      # config_key --> field_key --> { 'function': 'hash()',
      #                                 'args' : [ args ], ##  these are DERIVED entries, dictionaries
      #                                    [   { 'function': function_name, 'args' : [ XML Paths ]}  ]
      #                                 'order' : int }
  


    # LINK HASHED to HASHED????????
    # any hashes that use hashes? YES *** To Do **** might even be a bug in data_driven_parse.py !!!


    # SHOW ALL
    if True:
        merged_dict = {}
        merge_second_level_dict(merged_dict, base_field_dict)
        merge_second_level_dict(merged_dict, derived_field_dict)
        #merge_second_level_dict(merged_dict, hash_linked_to_base)
        #merge_second_level_dict(merged_dict, hash_linked_to_derived)
        print_data_hash(merged_dict)



if __name__ == '__main__':
    main()        
