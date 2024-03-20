
"""
    A stub for a proper vocabulary lookup facility.
    Also some convenient template id constants from the CCDA standards

    Maps HL7 codeSystem OIDs to more/less equivalent OMOP vocabulary_id
    Only more/less because OMOP doesn't track versions and so I think this
    could be a many-to-one mapping where multiple versions in HL7 map to
    whatever is in OMOP.
    Concepts in vocabularies here only need to be mapped from concept_code to concept_id.
"""
equivalent_vocab_map = {
   '2.16.840.1.113883.6.1': "LOINC",
   "http://snomed.info/sct": "SNOMED"
}


# vocabulary_id, concept_code--> name, concept_id
# Can be replaced with OMOP concept table:
omop_concept_ids = {
   ("SNOMED", "367336001"): ("Chemotherapy", 4273629),
   ("SNOMED", "387713003"): ("Surgical procedure", 4301351),
   ('LOINC', '30313-1'): ("Hemoglobin", 3002173),
   ('LOINC', '33765-9'): ("Leukocytes", 3028866),
   ('LOINC', '26515-7'): ("Platelets",  3007461)
}


# HL7: codeSyste, code --> OMOP: vocabulary_id, concept_code, name, concept_id
complex_mappings = {
   ('2.16.840.1.113883.5.1', 'F'): ("Gender", "FEMALE", 8532, "Gender", 'F'),
   ('2.16.840.1.113883.5.1', 'M'): ("Gender", "FEMALE", 8532, "Gender", 'F'),

   #  ("urn:oid:2.16.840.1.113883.6.238", "2106-3"): ("Race", "5", "White", 8527),
   ("2.16.840.1.113883.6.238", "2106-3"): ("Race", "5", "White", 8527),
   ("2.16.840.1.113883.6.238", None): ("Race", "1", "American Indian or Alaskan Native", 8657),
   #  ("2.16.840.1.113883.6.238", None): ("Race", "2", "Asian", 8515),
   #  ("2.16.840.1.113883.6.238", None): ("Race", "3", "Black or Afrian American", 8516),

   #  ("urn:oid:2.16.840.1.113883.6.238", "2186-5"):
   #       ("Ethnicity", "Not Hispanic", "Not Hispanic or Latino", 38003564),
   ("2.16.840.1.113883.6.238", "2186-5"):
   ("Ethnicity", "Not Hispanic", "Not Hispanic or Latino", 38003564),
   #  ("2.16.840.1.113883.6.238", None):
   #       ("Ethnicity", "Hispanic", "Hispanic or Latino", 9998, 38003563)
}


def map_hl7_to_omop(code_system, code):
    """ returns OMOP concept_id from HL7 codeSystem and code """
    # print(f"Looking for HL7 {code_system}:{code}")
    if code_system in equivalent_vocab_map:
        vocabulary_id = equivalent_vocab_map[code_system]
        # print(f"   got vocab:{vocabulary_id}")
        concept_id = omop_concept_ids[(vocabulary_id, code)][1]
        # print(f"   got concept_id:{concept_id}")
        return concept_id
    return complex_mappings[(code_system, code)][3]


# possible "domain" sections under this (prefix omitted) path:
# component/structuredBody/component/section/
# reason_for_referral="1.3.6.1.4.1.19376.1.5.3.1.3.1"
# medications        ="2.16.840.1.113883.10.20.22.2.1.1"
# immunizations      ="2.16.840.1.113883.10.20.22.2.2.1"
RESULTS = "2.16.840.1.113883.10.20.22.2.3.1"
# vital_signs        ="2.16.840.1.113883.10.20.22.2.4.1"
# problems           ="2.16.840.1.113883.10.20.22.2.5.1"
# allergies          ="2.16.840.1.113883.10.20.22.2.6.1"
# procedures         ="2.16.840.1.113883.10.20.22.2.7.1"
# care_plan          ="2.16.840.1.113883.10.20.22.2.10"
# functional_and_cognitive_status="2.16.840.1.113883.10.20.22.2.14"
# social_history     ="2.16.840.1.113883.10.20.22.2.17"
# encounters          ="2.16.840.1.113883.10.20.22.2.22.1"


# document template IDs
US_GENERAL_ROOT = "2.16.840.1.113883.10.20.22.1.1"
CCD_DOCUMENT_ROOT = "2.16.840.1.113883.10.20.22.1.2"  # I think.


# typeId?
# <typeId     root="2.16.840.1.113883.1.3" extension="POCD_HD000040"/>
