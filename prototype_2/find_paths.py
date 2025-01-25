
import re
import prototype_2.metadata

# none, constant,,,,derived, hash, priority
# not doing 'domain' fields

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
        print(f"    root:{root_path}")
        print("")
        for field_key in metadata[config_key]:
            print(f"    OMOP path {config_key}/{field_key}")
            if metadata[config_key][field_key]['config_type'] is None:
                base_field_dict[config_key][field_key] = 'None'
                print(f"    XML Path:'None'")
            if metadata[config_key][field_key]['config_type'] == 'CONSTANT':
                base_field_dict[config_key][field_key] = metadata[config_key][field_key]['constant_value']
                print(f"    XML Path:{metadata[config_key][field_key]['constant_value']}")
            if metadata[config_key][field_key]['config_type'] in ('FIELD', 'PK'):
                path=(f"{root_path}/"
                      f"{strip_detail(metadata[config_key][field_key]['element'])}"
                      f"@{strip_detail(metadata[config_key][field_key]['attribute'])}")
                base_field_dict[config_key][field_key] = path
                print(f"    XML Path:{path}/")
                print("")
        print("\n\n")

    return base_field_dict


def get_derived_fields(metadata):
    """
    Fetches keys of elements that are fetched from XML.
    The types of these keys are FIELD, PK.
    The keys are returned as config, field pairs.
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
                ##derived_field_dict[config_key][field_key]['function'] = name_of(metadata[config_key][field_key]['FUNCTION'])
                derived_field_dict[config_key][field_key]['function'] = getattr(metadata[config_key][field_key]['FUNCTION'], "__name__")
##getattr(func, "__name__", str(func))
                arg_keys= [ key for key in metadata[config_key][field_key]['argument_names'].keys() if key != 'default']
                #args = map(lambda key: metadata[config_key][field_key]['argument_names'][key], arg_keys)
                args=[]
                for arg_key in arg_keys:
                   args.append(metadata[config_key][field_key]['argument_names'][arg_key])
                derived_field_dict[config_key][field_key]['args'] = args
                print("")

    return derived_field_dict


def main():
    metadata = prototype_2.metadata.get_meta_dict()
    base_field_dict = get_base_elements(metadata)
    derived_field_dict = get_derived_fields(metadata) 
    print(derived_field_dict)


if __name__ == '__main__':
    main()        
