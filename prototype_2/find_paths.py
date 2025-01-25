
import prototype_2.metadata


def get_base_elements(metadata):
    """
    Fetches keys of elements that are fetched from XML.
    The types of these keys are FIELD, PK, HASH.
    The keys are returned as config, field pairs.
    """
    for config_key in metadata:
        root =  metadata[config_key]['root'] 
        print(f"config:{config_key}")
        for field_key in metadata[config_key]:
            if metadata[config_key][field_key]['config_type'] in ('FIELD', 'PK', 'HASH'):
                print(f"    config:{config_key} field:{field_key}")
                print(f"      type:{metadata[config_key][field_key]['config_type']}")
                print(f"    {metadata[config_key][field_key]}")
                print("")
        print("\n\n")


def main():
    metadata = prototype_2.metadata.get_meta_dict()
    get_base_elements(metadata)

if __name__ == '__main__':
    main()        
