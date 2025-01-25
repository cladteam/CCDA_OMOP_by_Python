
import re
import prototype_2.metadata

"""
field types:  none, constant,,,,derived, hash, priority

TODO:
- hash and priority
- domain
- track and filter 'order' attribute, recall it does both
  indicate that a field is part of the output, and tell
  what column it should be.
"""


def strip_detail(input_string):
    interim =  re.sub(r'hl7:', '', input_string)
    interim = re.sub(r'\[.*\]', '', interim)
    return interim

def get_base_elements(metadata):
    """
    Fetches keys of elements that are fetched from XML.
    The types of these keys are FIELD, PK.
    The keys are returned as config, field pairs.
    """

    base_field_dict = {}
    for config_key in metadata:
        base_field_dict[config_key] = {}
        root_path = metadata[config_key]['root']['element']
        root_path = strip_detail(root_path)
        #print(f"    root:{root_path}")
        #print("")
        for field_key in metadata[config_key]:
            #print(f"    OMOP path {config_key}/{field_key}")
            if metadata[config_key][field_key]['config_type'] is None:
                base_field_dict[config_key][field_key] = 'None'
                #print(f"    XML Path:'None'")
            if metadata[config_key][field_key]['config_type'] == 'CONSTANT':
                base_field_dict[config_key][field_key] = metadata[config_key][field_key]['constant_value']
                #print(f"    XML Path:{metadata[config_key][field_key]['constant_value']}")
            if metadata[config_key][field_key]['config_type'] in ('FIELD', 'PK'):
                path=(f"{root_path}/"
                      f"{strip_detail(metadata[config_key][field_key]['element'])}"
                      f"@{strip_detail(metadata[config_key][field_key]['attribute'])}")
                base_field_dict[config_key][field_key] = path
                #print(f"    XML Path:{path}/")
                #print("")
        #print("\n\n")

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
    """
    derived_field_dict = {}
        
    for config_key in metadata:
        derived_field_dict[config_key] = {}
        for field_key in metadata[config_key]:
            if metadata[config_key][field_key]['config_type'] == 'DERIVED':
                derived_field_dict[config_key][field_key] = {}
                derived_field_dict[config_key][field_key]['function'] = getattr(metadata[config_key][field_key]['FUNCTION'], "__name__")

                arg_keys= [ key for key in metadata[config_key][field_key]['argument_names'].keys() if key != 'default']
                #args = map(lambda key: metadata[config_key][field_key]['argument_names'][key], arg_keys)
                args=[]
                for arg_key in arg_keys:
                   args.append(metadata[config_key][field_key]['argument_names'][arg_key])
                derived_field_dict[config_key][field_key]['args'] = args

    return derived_field_dict


def link_derived_to_base(derived_field_dict, base_field_dict):
    # Assumes args, base fields,  for a derived field are in the same
    # config as the derived field.
    linked_field_dict = {}
    for der_config_key in derived_field_dict:
        linked_field_dict[der_config_key] = {}
        for der_field_key in derived_field_dict[der_config_key]:
            linked_field_dict[der_config_key][der_field_key] = {}
            func = derived_field_dict[der_config_key][der_field_key]['function']
            linked_field_dict[der_config_key][der_field_key]['function'] = func

            print(f"OMOP path: {der_config_key} field:{der_field_key}")
            print(f"   func:{func}") 
            linked_field_dict[der_config_key][der_field_key]['args'] = []
            for arg in derived_field_dict[der_config_key][der_field_key]['args']:
               xml_path = base_field_dict[der_config_key][arg]
               linked_field_dict[der_config_key][der_field_key]['args'].append(xml_path)
               print(f"   arg:{arg} {xml_path}") 
            print("")

    return linked_field_dict

def main():
    metadata = prototype_2.metadata.get_meta_dict()
    # config_key --> field_key --> dict

    base_field_dict = get_base_elements(metadata)
    # config_key --> field_key --> XML Path
    
    derived_field_dict = get_derived_fields(metadata) 
    # config_key --> field_key --> function -> function name
    # config_key --> field_key --> args -> [ field name ]

    derived_linked_to_base = link_derived_to_base(derived_field_dict, base_field_dict)
    # config_key --> derived_field_key --> function 
    # config_key --> derived_field_key --> [ base_field_key --> XML Path ]
    for config_key in derived_linked_to_base:
        for field_key in derived_linked_to_base[config_key]:
            for thing_key in derived_linked_to_base[config_key][field_key]:
                print(f"{config_key} {field_key} {thing_key} {derived_linked_to_base[config_key][field_key][thing_key]}")
        print("")

if __name__ == '__main__':
    main()        
