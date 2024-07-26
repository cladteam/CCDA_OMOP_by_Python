
def map_oid(vocabuarly_oid):
    """ maps an OID used in CCDA to indicate the vocabulary
        to an OMOP vocabulary_id.
        FIX: needs the data, needs written
    """
    return 1
    
def map_to_omop_concept_id(vocabulary_id, concept_code):
    """ Simply maps vocabulary_id, concept_code to an OMOP concept_id.
        FIX: needs the data, needs written.
        Somewhat redundant in that map_to_standard_omop_concept_id
        is much more useful and likely to be used.
    """
    return 2

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
    #vocabulary_id = map_oid(args_dict['vocabulary_oid'])
    #concept_id = map_to_standard_omop_concept_id(vocabulary_id, args_dict['concept_code'])
    #return concept_id
    return 123456

def map_hl7_to_omop_w_dict_args(args_dict):
    """ expects: vocabulary_oid, concept_code
        FIX: needs the data, needs written
    """
    return map_hl7_to_omop(args_dict['vocabulary_oid'], args_dict['concept_code'])


